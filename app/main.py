from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import asyncio
import httpx
import asyncpg
import clickhouse_connect
from kubernetes import client as k8s_client, config as k8s_config
from kubernetes.stream import stream as k8s_stream
from datetime import datetime
import re
import os
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

ZOOKEEPER_HOST      = os.getenv("ZOOKEEPER_HOST", "kafka-cluster-cp-zookeeper")
ZOOKEEPER_PORT      = int(os.getenv("ZOOKEEPER_PORT", "2181"))
K8S_NAMESPACE       = os.getenv("K8S_NAMESPACE", "vsmaps")

# Exact pod names for exec commands
KAFKA_POD           = os.getenv("KAFKA_POD",      "kafka-cluster-cp-kafka-0")
ZOOKEEPER_POD       = os.getenv("ZOOKEEPER_POD",  "kafka-cluster-cp-zookeeper-0")

# Pod prefix lists for health gate (comma-separated, supports partial match)
KAFKA_PODS          = os.getenv("KAFKA_PODS",         "kafka-cluster-cp-kafka-0").split(",")
ZOOKEEPER_PODS      = os.getenv("ZOOKEEPER_PODS",     "kafka-cluster-cp-zookeeper-0").split(",")
KAFKA_CONNECT_PODS  = os.getenv("KAFKA_CONNECT_PODS", "connect").split(",")

POSTGRES_DSN        = os.getenv("POSTGRES_DSN",   "postgresql://user:password@postgres:5432/appdb")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

MONITORED_PODS      = os.getenv("MONITORED_PODS", "denver,nairobi,broker,kafka-cluster-cp-zookeeper,chi-clickhouse,postgresql,minio-tenant,keycloak,traefik").split(",")
KAFKA_LAG_THRESHOLD = int(os.getenv("KAFKA_LAG_THRESHOLD", "10000"))

# ─── State ───────────────────────────────────────────────────────────────────
last_result:      dict = {}
last_checked:     str  = ""
is_running:       bool = False
_offset_snapshot: dict = {}


# ═══════════════════════════════════════════════════════════════════════════════
# K8S HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _load_k8s():
    try:
        k8s_config.load_incluster_config()
    except Exception:
        k8s_config.load_kube_config()
    return k8s_client.CoreV1Api()


def _k8s_exec(pod_name: str, namespace: str, command: list) -> str:
    v1 = _load_k8s()
    resp = k8s_stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name, namespace,
        command=command,
        stderr=True, stdin=False, stdout=True, tty=False,
        _preload_content=True,
    )
    return resp if isinstance(resp, str) else ""


def _fetch_pod_logs(v1, pod_name: str, namespace: str, tail: int = 30) -> str:
    """Fetch last N lines of logs. Falls back to previous container logs for crashloop."""
    try:
        logs = v1.read_namespaced_pod_log(
            name=pod_name, namespace=namespace,
            tail_lines=tail, timestamps=False
        )
        return logs.strip()
    except Exception:
        try:
            logs = v1.read_namespaced_pod_log(
                name=pod_name, namespace=namespace,
                tail_lines=tail, previous=True
            )
            return f"[Previous container logs]\n{logs.strip()}"
        except Exception as e:
            return f"Could not fetch logs: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# POD HEALTH GATE
# ═══════════════════════════════════════════════════════════════════════════════

def _get_pod_health(pod) -> dict:
    """
    Inspect a pod object and return rich health info.
    Covers: Running, Pending, CrashLoopBackOff, OOMKilled, Error,
            Terminating, ImagePullBackOff, ContainerCreating etc.
    """
    name  = pod.metadata.name
    phase = pod.status.phase or "Unknown"

    result = {
        "name":     name,
        "phase":    phase,
        "status":   "ok",
        "reason":   "",
        "restarts": 0,
        "message":  "",
        "logs":     "",
    }

    # Terminating
    if pod.metadata.deletion_timestamp:
        result["status"] = "warn"
        result["reason"] = "Terminating"
        return result

    all_cs = (pod.status.init_container_statuses or []) + (pod.status.container_statuses or [])

    for c in all_cs:
        result["restarts"] += c.restart_count or 0
        state = c.state

        if state and state.waiting:
            reason = state.waiting.reason or ""
            msg    = state.waiting.message or ""
            if reason == "CrashLoopBackOff":
                result["status"]  = "error"
                result["reason"]  = "CrashLoopBackOff"
                result["message"] = msg or "Container keeps crashing and restarting"
            elif reason == "OOMKilled":
                result["status"]  = "error"
                result["reason"]  = "OOMKilled"
                result["message"] = "Container killed — Out Of Memory"
            elif reason in ("Error", "CreateContainerError", "CreateContainerConfigError",
                            "InvalidImageName", "ImagePullBackOff", "ErrImagePull"):
                result["status"]  = "error"
                result["reason"]  = reason
                result["message"] = msg
            elif reason in ("PodInitializing", "ContainerCreating"):
                if result["status"] == "ok":
                    result["status"] = "warn"
                    result["reason"] = reason

        if state and state.terminated:
            reason = state.terminated.reason or ""
            code   = state.terminated.exit_code
            if reason == "OOMKilled":
                result["status"]  = "error"
                result["reason"]  = "OOMKilled"
                result["message"] = f"Exit code {code} — killed by OOM killer"
            elif code != 0 and result["status"] == "ok":
                result["status"]  = "error"
                result["reason"]  = f"Terminated (exit {code})"

        # Check last state for previous OOMKill
        last = c.last_state
        if last and last.terminated and last.terminated.reason == "OOMKilled":
            if result["status"] == "ok":
                result["status"]  = "warn"
                result["reason"]  = "Previously OOMKilled"
                result["message"] = f"Last restart was OOM. Total restarts: {result['restarts']}"

    # High restart count
    if result["restarts"] >= 5 and result["status"] == "ok":
        result["status"] = "warn"
        result["reason"] = f"High restart count ({result['restarts']})"

    # Phase-level fallback
    if phase not in ("Running", "Succeeded") and result["status"] == "ok":
        result["status"] = "error" if phase in ("Failed", "Unknown") else "warn"
        result["reason"] = phase

    return result


def check_kafka_pods(v1) -> dict:
    """
    Gate check — runs BEFORE any Kafka diagnostics.
    Finds all Kafka broker, Zookeeper, and Connect pods.
    Returns pod health for each and whether all are healthy.

    Structure:
    {
      "all_healthy": bool,
      "broker_ready": bool,
      "zookeeper_ready": bool,
      "connect_ready": bool,
      "pods": { pod_name: health_dict },
      "unhealthy": [ pod_name ]
    }
    """
    pod_list = v1.list_namespaced_pod(namespace=K8S_NAMESPACE)
    pod_map  = {p.metadata.name: p for p in pod_list.items}

    def find_pods(prefixes: list) -> list:
        found = []
        for prefix in prefixes:
            prefix = prefix.strip()
            if prefix in pod_map:
                found.append(pod_map[prefix])
            else:
                matches = [p for n, p in pod_map.items() if prefix.lower() in n.lower()]
                found.extend(matches)
        # Deduplicate
        seen = set()
        deduped = []
        for p in found:
            if p.metadata.name not in seen:
                seen.add(p.metadata.name)
                deduped.append(p)
        return deduped

    kafka_pods   = find_pods(KAFKA_PODS)
    zk_pods      = find_pods(ZOOKEEPER_PODS)
    connect_pods = find_pods(KAFKA_CONNECT_PODS)

    result = {
        "all_healthy":      True,
        "broker_ready":     False,
        "zookeeper_ready":  False,
        "connect_ready":    False,
        "pods":             {},
        "unhealthy":        [],
    }

    def process_pods(pods, ready_key):
        group_healthy = True
        for pod in pods:
            health = _get_pod_health(pod)
            # Fetch logs for any non-ok pod
            if health["status"] in ("error", "warn"):
                health["logs"] = _fetch_pod_logs(v1, pod.metadata.name, K8S_NAMESPACE, tail=30)
            result["pods"][pod.metadata.name] = health
            if health["status"] == "error":
                result["unhealthy"].append(pod.metadata.name)
                group_healthy = False
        if pods:
            result[ready_key] = group_healthy
        return group_healthy

    broker_ok  = process_pods(kafka_pods,   "broker_ready")
    zk_ok      = process_pods(zk_pods,      "zookeeper_ready")
    connect_ok = process_pods(connect_pods, "connect_ready")

    # Check for completely missing required pods
    for prefix in KAFKA_PODS + ZOOKEEPER_PODS:
        prefix = prefix.strip()
        found  = any(prefix.lower() in n.lower() for n in result["pods"])
        if not found:
            result["pods"][prefix] = {
                "name":     prefix,
                "phase":    "Missing",
                "status":   "error",
                "reason":   "Pod not found",
                "restarts": 0,
                "message":  f"Expected pod '{prefix}' not found in namespace {K8S_NAMESPACE}",
                "logs":     "",
            }
            result["unhealthy"].append(prefix)
            if prefix in [p.strip() for p in KAFKA_PODS]:
                result["broker_ready"] = False
            if prefix in [p.strip() for p in ZOOKEEPER_PODS]:
                result["zookeeper_ready"] = False

    result["all_healthy"] = broker_ok and zk_ok
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PARSERS
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_topic_describe(raw: str) -> dict:
    topics = {}
    current_topic = None
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("Topic:") and "Partition:" not in line:
            m = re.match(r"Topic:\s+(\S+)\s+PartitionCount:\s+(\d+)\s+ReplicationFactor:\s+(\d+)\s*(?:Configs:\s*(.*))?", line)
            if m:
                current_topic = m.group(1)
                if current_topic.startswith("__"):
                    current_topic = None
                    continue
                configs = {}
                for cfg in (m.group(4) or "").split(","):
                    if "=" in cfg:
                        k, v = cfg.strip().split("=", 1)
                        configs[k.strip()] = v.strip()
                topics[current_topic] = {
                    "partition_count":    int(m.group(2)),
                    "replication_factor": int(m.group(3)),
                    "configs":            configs,
                    "partitions":         [],
                    "under_replicated":   False,
                    "offline_partitions": [],
                }
        elif line.startswith("Topic:") and "Partition:" in line and current_topic:
            m = re.match(r"Topic:\s+\S+\s+Partition:\s+(\d+)\s+Leader:\s+(-?\d+)\s+Replicas:\s+([\d,]+)\s+Isr:\s+([\d,]*)", line)
            if m:
                replicas = [r for r in m.group(3).split(",") if r]
                isr      = [r for r in m.group(4).split(",") if r]
                leader   = int(m.group(2))
                topics[current_topic]["partitions"].append({
                    "id": int(m.group(1)), "leader": leader,
                    "replicas": replicas, "isr": isr,
                    "isr_count": len(isr), "replica_count": len(replicas),
                    "is_healthy": len(isr) == len(replicas) and leader != -1,
                })
                if len(isr) < len(replicas):
                    topics[current_topic]["under_replicated"] = True
                if leader == -1:
                    topics[current_topic]["offline_partitions"].append(int(m.group(1)))
    return topics


def _parse_offsets(raw: str) -> dict:
    result = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(":", 2)
        if len(parts) == 3:
            topic, partition, offset = parts
            if not topic.startswith("__"):
                try:
                    result[f"{topic}:{partition}"] = int(offset)
                except ValueError:
                    pass
    return result


def _parse_consumer_groups(raw: str) -> dict:
    topic_lag = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("GROUP") or line.startswith("Consumer") or line.startswith("Error"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        group_id, topic, partition, lag_str = parts[0], parts[1], parts[2], parts[5]
        if topic.startswith("__"):
            continue
        try:
            lag = int(lag_str)
        except ValueError:
            continue
        if topic not in topic_lag:
            topic_lag[topic] = {"total_lag": 0, "max_lag": 0, "groups": {}}
        topic_lag[topic]["total_lag"] += lag
        topic_lag[topic]["max_lag"]    = max(topic_lag[topic]["max_lag"], lag)
        if group_id not in topic_lag[topic]["groups"]:
            topic_lag[topic]["groups"][group_id] = []
        topic_lag[topic]["groups"][group_id].append({"partition": partition, "lag": lag})
    return topic_lag


def _parse_zk_stat(raw: str) -> dict:
    info = {}
    for line in raw.splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            info[k.strip()] = v.strip()
    return info


def _parse_zk_mntr(raw: str) -> dict:
    info = {}
    for line in raw.splitlines():
        if "\t" in line:
            k, _, v = line.partition("\t")
            info[k.strip()] = v.strip()
    return info


# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA CHECK — Pod gate first, then diagnostics
# ═══════════════════════════════════════════════════════════════════════════════

async def check_kafka() -> dict:
    checks  = {}
    details = {}
    loop    = asyncio.get_event_loop()

    # ── STEP 1: Pod Health Gate ───────────────────────────────────────────────
    try:
        v1          = await loop.run_in_executor(None, _load_k8s)
        pod_health  = await loop.run_in_executor(None, lambda: check_kafka_pods(v1))
    except Exception as e:
        checks["Pod Health"] = {"status": "error", "detail": f"Could not check pods: {e}"}
        return checks

    details["pod_health"] = pod_health

    # Build per-pod status checks for UI display
    for pod_name, ph in pod_health["pods"].items():
        icon = "🟢" if ph["status"] == "ok" else ("🔴" if ph["status"] == "error" else "🟡")
        detail_parts = [f"Phase: {ph['phase']}"]
        if ph["reason"]:   detail_parts.append(ph["reason"])
        if ph["restarts"]: detail_parts.append(f"Restarts: {ph['restarts']}")
        if ph["message"]:  detail_parts.append(ph["message"])

        checks[f"Pod: {pod_name}"] = {
            "status": ph["status"],
            "detail": f"{icon} " + " | ".join(detail_parts),
            "logs":   ph.get("logs", ""),
        }

    # ── STEP 2: Gate — skip diagnostics if broker or ZK not healthy ──────────
    if not pod_health["broker_ready"]:
        checks["Broker"] = {
            "status": "error",
            "detail": "⛔ Skipping broker diagnostics — Kafka broker pod is not healthy. Fix pod issues above first."
        }
        checks["Topics"]        = {"status": "unknown", "detail": "Skipped — broker not ready"}
        checks["Live Data"]     = {"status": "unknown", "detail": "Skipped — broker not ready"}
        checks["Consumer Lag"]  = {"status": "unknown", "detail": "Skipped — broker not ready"}
        checks["Cluster ID"]    = {"status": "unknown", "detail": "Skipped — broker not ready"}
        checks["ISR Sync"]      = {"status": "unknown", "detail": "Skipped — broker not ready"}
        checks["__details__"]   = details
        return checks

    if not pod_health["zookeeper_ready"]:
        checks["Zookeeper Health"] = {
            "status": "error",
            "detail": "⛔ Skipping ZK diagnostics — Zookeeper pod is not healthy. Fix pod issues above first."
        }

    # ── STEP 3: Broker Alive ──────────────────────────────────────────────────
    try:
        raw = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
        ))
        if "usable" in raw or "-> (" in raw:
            m = re.search(r"(\S+):9092 \(id: (\d+)", raw)
            info = f"Broker ID: {m.group(2)} | Host: {m.group(1)}" if m else "Broker responding"
            checks["Broker"] = {"status": "ok", "detail": info}
        else:
            checks["Broker"] = {"status": "error", "detail": "No API response from broker"}
    except Exception as e:
        checks["Broker"] = {"status": "error", "detail": str(e)}

    # ── STEP 4: Cluster ID Match ──────────────────────────────────────────────
    try:
        meta = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE, ["cat", "/var/lib/kafka/data/meta.properties"]
        ))
        broker_id = next((l.split("=", 1)[1].strip() for l in meta.splitlines() if l.startswith("cluster.id=")), None)

        zk_raw = await loop.run_in_executor(None, lambda: _k8s_exec(
            ZOOKEEPER_POD, K8S_NAMESPACE,
            ["zookeeper-shell", "localhost:2181", "get", "/cluster/id"]
        ))
        m = re.search(r'"id"\s*:\s*"([^"]+)"', zk_raw)
        zk_id = m.group(1) if m else None

        if broker_id and zk_id:
            if broker_id == zk_id:
                checks["Cluster ID"] = {"status": "ok",    "detail": f"Match ✓ — {broker_id}"}
            else:
                checks["Cluster ID"] = {
                    "status": "error",
                    "detail": f"⚠️ MISMATCH! Broker: {broker_id} | ZK: {zk_id} — Risk of CrashLoopBackOff!"
                }
        else:
            checks["Cluster ID"] = {"status": "warn", "detail": f"Could not verify (broker:{broker_id}, zk:{zk_id})"}
    except Exception as e:
        checks["Cluster ID"] = {"status": "warn", "detail": str(e)}

    # ── STEP 5: Topics ────────────────────────────────────────────────────────
    topic_data = {}
    try:
        raw        = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-topics", "--bootstrap-server", "localhost:9092", "--describe"]
        ))
        topic_data        = _parse_topic_describe(raw)
        under_replicated  = [t for t, d in topic_data.items() if d["under_replicated"]]
        offline_topics    = [t for t, d in topic_data.items() if d["offline_partitions"]]

        if offline_topics:
            checks["Topics"] = {"status": "error", "detail": f"{len(topic_data)} topics | {len(offline_topics)} have OFFLINE partitions: {', '.join(offline_topics[:3])}"}
        elif under_replicated:
            checks["Topics"] = {"status": "warn",  "detail": f"{len(topic_data)} topics | {len(under_replicated)} under-replicated: {', '.join(under_replicated[:3])}"}
        else:
            checks["Topics"] = {"status": "ok",    "detail": f"{len(topic_data)} topics — all partitions healthy"}
        details["topics"] = topic_data
    except Exception as e:
        checks["Topics"] = {"status": "error", "detail": str(e)}

    # ── STEP 6: Live Data ─────────────────────────────────────────────────────
    global _offset_snapshot
    try:
        raw_latest   = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-get-offsets", "--bootstrap-server", "localhost:9092", "--time", "latest"]
        ))
        raw_earliest = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-get-offsets", "--bootstrap-server", "localhost:9092", "--time", "earliest"]
        ))
        latest_offsets   = _parse_offsets(raw_latest)
        earliest_offsets = _parse_offsets(raw_earliest)
        now              = datetime.now().timestamp()

        topic_live = {}
        for key, latest in latest_offsets.items():
            topic = key.rsplit(":", 1)[0]
            if topic not in topic_live:
                topic_live[topic] = {"has_data": False, "is_live": False, "total_messages": 0}
            msgs = latest - earliest_offsets.get(key, 0)
            topic_live[topic]["total_messages"] += msgs
            if msgs > 0:
                topic_live[topic]["has_data"] = True
            if key in _offset_snapshot and latest > _offset_snapshot[key][0]:
                topic_live[topic]["is_live"] = True

        for key, offset in latest_offsets.items():
            _offset_snapshot[key] = (offset, now)

        live_topics  = [t for t, s in topic_live.items() if s["is_live"]]
        stale_topics = [t for t, s in topic_live.items() if s["has_data"] and not s["is_live"]]
        empty_topics = [t for t, s in topic_live.items() if not s["has_data"]]

        parts = [f"{len(live_topics)} live"]
        if stale_topics: parts.append(f"{len(stale_topics)} stale (no new msgs)")
        if empty_topics:  parts.append(f"{len(empty_topics)} empty")

        checks["Live Data"] = {
            "status": "ok" if not stale_topics else "warn",
            "detail": " | ".join(parts)
        }
        details["topic_live_status"] = topic_live
    except Exception as e:
        checks["Live Data"] = {"status": "warn", "detail": str(e)}

    # ── STEP 7: Consumer Lag ──────────────────────────────────────────────────
    lag_data = {}
    try:
        raw      = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
             "--describe", "--all-groups"]
        ))
        lag_data        = _parse_consumer_groups(raw)
        high_lag_topics = {t: d for t, d in lag_data.items() if d["total_lag"] > KAFKA_LAG_THRESHOLD}

        if high_lag_topics:
            top = sorted(high_lag_topics.items(), key=lambda x: x[1]["total_lag"], reverse=True)[:3]
            checks["Consumer Lag"] = {
                "status": "warn",
                "detail": "⚠️ HIGH LAG: " + ", ".join([f"{t} ({d['total_lag']:,})" for t, d in top])
            }
        else:
            total_groups = len(set(g for d in lag_data.values() for g in d["groups"]))
            checks["Consumer Lag"] = {
                "status": "ok",
                "detail": f"{total_groups} consumer groups — all within threshold (<{KAFKA_LAG_THRESHOLD:,})"
            }
        details["consumer_lag"] = lag_data
    except Exception as e:
        checks["Consumer Lag"] = {"status": "warn", "detail": str(e)}

    # ── STEP 8: ISR Sync ──────────────────────────────────────────────────────
    try:
        if topic_data:
            under = sum(1 for d in topic_data.values() if d["under_replicated"])
            checks["ISR Sync"] = {
                "status": "ok"   if under == 0 else "warn",
                "detail": "All partitions fully in-sync" if under == 0 else f"{under} topics under-replicated (ISR < Replicas)"
            }
    except Exception as e:
        checks["ISR Sync"] = {"status": "warn", "detail": str(e)}

    # ── STEP 9: Zookeeper ─────────────────────────────────────────────────────
    if pod_health["zookeeper_ready"]:
        try:
            zk_ruok = await loop.run_in_executor(None, lambda: _k8s_exec(
                ZOOKEEPER_POD, K8S_NAMESPACE, ["bash", "-c", "echo ruok | nc localhost 2181"]
            ))
            checks["Zookeeper Health"] = {
                "status": "ok"    if "imok" in zk_ruok else "error",
                "detail": "imok ✓" if "imok" in zk_ruok else f"Bad response: {zk_ruok.strip()}"
            }

            zk_stat = await loop.run_in_executor(None, lambda: _k8s_exec(
                ZOOKEEPER_POD, K8S_NAMESPACE, ["bash", "-c", "echo stat | nc localhost 2181"]
            ))
            stat = _parse_zk_stat(zk_stat)
            checks["Zookeeper Mode"] = {
                "status": "ok",
                "detail": f"Mode: {stat.get('Mode','?')} | Connections: {stat.get('Connections','?')} | Latency: {stat.get('Latency min/avg/max','?')}"
            }

            zk_mntr = await loop.run_in_executor(None, lambda: _k8s_exec(
                ZOOKEEPER_POD, K8S_NAMESPACE, ["bash", "-c", "echo mntr | nc localhost 2181"]
            ))
            mntr    = _parse_zk_mntr(zk_mntr)
            avg_lat = float(mntr.get("zk_avg_latency", "0") or "0")
            out_req = int(mntr.get("zk_outstanding_requests", "0") or "0")
            zk_s    = "warn" if avg_lat > 100 or out_req > 10 else "ok"
            zk_d    = f"Znodes: {mntr.get('zk_znode_count','?')} | Avg latency: {avg_lat}ms | Outstanding: {out_req}"
            if avg_lat > 100: zk_d += " ⚠️ High latency"
            if out_req > 10:  zk_d += " ⚠️ High outstanding requests"
            checks["Zookeeper Stats"] = {"status": zk_s, "detail": zk_d}
            details["zookeeper"] = {
                "mode": stat.get("Mode", "?"),
                "connections": stat.get("Connections", "?"),
                "znode_count": mntr.get("zk_znode_count", "?"),
                "avg_latency": avg_lat,
                "outstanding_requests": out_req,
            }
        except Exception as e:
            checks["Zookeeper Health"] = {"status": "error", "detail": str(e)}

    checks["__details__"] = details
    return checks


# ═══════════════════════════════════════════════════════════════════════════════
# TOPIC DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════════

async def get_topic_detail(topic_name: str) -> dict:
    result = {"topic": topic_name, "found": False}
    loop   = asyncio.get_event_loop()
    try:
        raw = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-topics", "--bootstrap-server", "localhost:9092",
             "--describe", "--topic", topic_name]
        ))
        if "does not exist" in raw or not raw.strip():
            result["error"] = f"Topic '{topic_name}' not found"
            return result

        topic_data = _parse_topic_describe(raw)
        if topic_name not in topic_data:
            result["error"] = f"Could not parse data for '{topic_name}'"
            return result

        result["found"] = True
        result["info"]  = topic_data[topic_name]

        raw_latest   = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-get-offsets", "--bootstrap-server", "localhost:9092",
             "--time", "latest", "--topic", topic_name]
        ))
        raw_earliest = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-get-offsets", "--bootstrap-server", "localhost:9092",
             "--time", "earliest", "--topic", topic_name]
        ))
        latest_offsets   = _parse_offsets(raw_latest)
        earliest_offsets = _parse_offsets(raw_earliest)

        total_messages    = 0
        partition_offsets = []
        for key, latest in latest_offsets.items():
            if not key.startswith(topic_name + ":"):
                continue
            partition = key.split(":")[1]
            earliest  = earliest_offsets.get(key, 0)
            msgs      = latest - earliest
            total_messages += msgs
            is_live   = key in _offset_snapshot and latest > _offset_snapshot[key][0]
            partition_offsets.append({
                "partition": partition, "earliest": earliest,
                "latest": latest, "messages": msgs, "is_live": is_live,
            })

        result["total_messages"]    = total_messages
        result["partition_offsets"] = partition_offsets

        raw_lag  = await loop.run_in_executor(None, lambda: _k8s_exec(
            KAFKA_POD, K8S_NAMESPACE,
            ["kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
             "--describe", "--all-groups"]
        ))
        lag_data = _parse_consumer_groups(raw_lag)
        result["lag"] = lag_data.get(topic_name, {})

        configs   = topic_data[topic_name].get("configs", {})
        ret_ms    = configs.get("retention.ms", "Broker default")
        ret_bytes = configs.get("retention.bytes", "-1")
        if ret_ms not in ("Broker default",):
            try:
                days   = int(ret_ms) / (1000 * 60 * 60 * 24)
                ret_ms = f"{ret_ms}ms ({days:.1f} days)"
            except ValueError:
                pass
        result["retention"] = {"retention_ms": ret_ms, "retention_bytes": ret_bytes}

    except Exception as e:
        result["error"] = str(e)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# OTHER SERVICE CHECKS
# ═══════════════════════════════════════════════════════════════════════════════

async def check_clickhouse() -> dict:
    checks = {}
    try:
        c = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, connect_timeout=5
        )
        c.query("SELECT 1")
        checks["Connection"] = {"status": "ok", "detail": "Reachable"}
        try:
            t = c.query("SELECT count() FROM system.tables")
            checks["System Tables"] = {"status": "ok", "detail": f"{t.result_rows[0][0]} tables"}
        except Exception as e:
            checks["System Tables"] = {"status": "warn", "detail": str(e)}
        checks["Query Execution"] = {"status": "ok", "detail": "Queries executing normally"}
        c.close()
    except Exception as e:
        checks["Connection"]      = {"status": "error",   "detail": str(e)}
        checks["System Tables"]   = {"status": "unknown", "detail": "Skipped"}
        checks["Query Execution"] = {"status": "unknown", "detail": "Skipped"}
    return checks


async def check_postgres() -> dict:
    checks = {}
    try:
        conn = await asyncio.wait_for(asyncpg.connect(POSTGRES_DSN), timeout=5)
        checks["Connection"] = {"status": "ok", "detail": "Connected successfully"}
        try:
            v = await conn.fetchval("SELECT version()")
            checks["Version"] = {"status": "ok", "detail": v.split(",")[0]}
        except Exception as e:
            checks["Version"] = {"status": "warn", "detail": str(e)}
        try:
            await conn.fetchval("SELECT 1")
            checks["Query Execution"] = {"status": "ok", "detail": "Queries running normally"}
        except Exception as e:
            checks["Query Execution"] = {"status": "error", "detail": str(e)}
        await conn.close()
    except Exception as e:
        checks["Connection"]      = {"status": "error",   "detail": str(e)}
        checks["Version"]         = {"status": "unknown", "detail": "Skipped"}
        checks["Query Execution"] = {"status": "unknown", "detail": "Skipped"}
    return checks


async def check_minio() -> dict:
    checks = {}
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r  = await http.get(f"{MINIO_ENDPOINT}/minio/health/live")
            r2 = await http.get(f"{MINIO_ENDPOINT}/minio/health/ready")
            checks["Liveness"]  = {"status": "ok" if r.status_code  == 200 else "warn", "detail": "MinIO is live"  if r.status_code  == 200 else f"Status {r.status_code}"}
            checks["Readiness"] = {"status": "ok" if r2.status_code == 200 else "warn", "detail": "MinIO is ready" if r2.status_code == 200 else f"Status {r2.status_code}"}
    except Exception as e:
        checks["Liveness"]  = {"status": "error",   "detail": str(e)}
        checks["Readiness"] = {"status": "unknown", "detail": "Skipped"}
    return checks


async def check_kubernetes() -> dict:
    checks = {}
    try:
        v1      = _load_k8s()
        pods    = v1.list_namespaced_pod(namespace=K8S_NAMESPACE)
        pod_map = {p.metadata.name: p for p in pods.items}
        for pod_prefix in MONITORED_PODS:
            pod_prefix = pod_prefix.strip()
            matching   = [p for name, p in pod_map.items() if pod_prefix.lower() in name.lower()]
            if not matching:
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "error", "detail": "No pod found"}
                continue
            pod   = matching[0]
            phase = pod.status.phase
            name  = pod.metadata.name
            cs    = pod.status.container_statuses or []
            if phase == "Running":
                restarts  = sum(c.restart_count for c in cs)
                all_ready = all(c.ready for c in cs)
                if all_ready and restarts < 5:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "ok",   "detail": f"{name} — Running, {restarts} restarts"}
                elif restarts >= 5:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn", "detail": f"{name} — {restarts} restarts (high)"}
                else:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn", "detail": f"{name} — Not all containers ready"}
            elif phase == "Pending":
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn",  "detail": f"{name} — Pending"}
            else:
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "error", "detail": f"{name} — {phase}"}
        nodes       = v1.list_node()
        ready_nodes = sum(1 for n in nodes.items for c in n.status.conditions if c.type == "Ready" and c.status == "True")
        checks["Cluster Nodes"] = {
            "status": "ok" if ready_nodes > 0 else "error",
            "detail": f"{ready_nodes}/{len(nodes.items)} nodes Ready"
        }
    except Exception as e:
        checks["Kubernetes API"] = {"status": "error", "detail": str(e)}
    return checks


# ═══════════════════════════════════════════════════════════════════════════════
# AGGREGATE RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

async def run_all_checks():
    global last_result, last_checked, is_running
    if is_running:
        return
    is_running = True
    logger.info("Starting health checks...")
    try:
        results = await asyncio.gather(
            check_clickhouse(), check_kafka(), check_postgres(),
            check_minio(), check_kubernetes(), return_exceptions=True
        )
        labels      = ["clickhouse", "kafka", "postgres", "minio", "kubernetes"]
        last_result = {}
        for label, res in zip(labels, results):
            last_result[label] = {"Error": {"status": "error", "detail": str(res)}} if isinstance(res, Exception) else res
        last_checked = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Health checks completed at {last_checked}")

        # TEMP DEBUG — remove after verification
        import json
        logger.info("=== FULL HEALTH CHECK OUTPUT ===")
        logger.info(json.dumps(last_result, indent=2, default=str))
        logger.info("=== END OUTPUT ===")
    finally:
        is_running = False


# ═══════════════════════════════════════════════════════════════════════════════
# APP + ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(run_all_checks())
    scheduler.add_job(run_all_checks, "cron", hour="8,20", minute=0)
    scheduler.start()
    yield
    scheduler.shutdown()

app       = FastAPI(title="HealthWatch", lifespan=lifespan, root_path="/healthwatch")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return {"last_checked": last_checked, "is_running": is_running, "results": last_result}

@app.post("/api/run")
async def trigger_checks(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_checks)
    return {"message": "Health checks triggered"}

@app.get("/api/topic/{topic_name}")
async def topic_detail(topic_name: str):
    return await get_topic_detail(topic_name)

@app.get("/api/kafka/topics")
async def all_topics():
    kafka_data = last_result.get("kafka", {})
    details    = kafka_data.get("__details__", {})
    return {
        "topics":            details.get("topics", {}),
        "topic_live_status": details.get("topic_live_status", {}),
        "consumer_lag":      details.get("consumer_lag", {}),
        "lag_threshold":     KAFKA_LAG_THRESHOLD,
    }
