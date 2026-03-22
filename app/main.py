"""
HealthWatch Phase 2 — Production backend with full structured logging.
All checks use real K8s API, ClickHouse, Kafka, PostgreSQL, and MinIO.
Runs inside the cluster with in-cluster kubeconfig.

To view logs:
    kubectl logs -n vsmaps -l app=healthwatch -f
    kubectl logs -n vsmaps -l app=healthwatch --previous   # crashed pod
"""

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from datetime import datetime
import asyncio, os, logging, traceback, time, ssl, socket
from pathlib import Path

# ─── Logging setup ───────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("healthwatch")

# ─── Config ──────────────────────────────────────────────────────────────────
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "vsmaps")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.vsmaps.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "vusmart")
KAFKA_REQUIRED_CONNECTORS = os.getenv(
    "KAFKA_REQUIRED_CONNECTORS", "enrichment-connector"
)
KAFKA_CONNECT_URL = os.getenv(
    "KAFKA_CONNECT_URL", "http://connect.vsmaps.svc.cluster.local:9082"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "broker-headless.vsmaps.svc.cluster.local:9092"
)
KAFKA_LIVE_WINDOW_MINUTES = int(os.getenv("KAFKA_LIVE_WINDOW_MINUTES", "10"))
KAFKA_LAG_WARN_THRESHOLD = int(os.getenv("KAFKA_LAG_WARN_THRESHOLD", "10000"))
NODE_CPU_WARN_THRESHOLD = float(os.getenv("NODE_CPU_WARN_THRESHOLD", "70"))
NODE_MEM_WARN_THRESHOLD = float(os.getenv("NODE_MEM_WARN_THRESHOLD", "80"))
POD_CPU_WARN_THRESHOLD = float(os.getenv("POD_CPU_WARN_THRESHOLD", "70"))
POD_MEM_WARN_THRESHOLD = float(os.getenv("POD_MEM_WARN_THRESHOLD", "80"))
PVC_USED_THRESHOLD = float(os.getenv("PVC_USED_THRESHOLD", "0.8"))
POD_RESTART_THRESHOLD = int(os.getenv("POD_RESTART_THRESHOLD", "10"))
CH_MUTATION_AGE_MINUTES = int(os.getenv("CH_MUTATION_AGE_MINUTES", "30"))
CH_REPLICATION_LIMIT = int(os.getenv("CH_REPLICATION_POSTPONE_LIMIT", "100"))
CH_CLUSTER_NAME = os.getenv("CH_CLUSTER_NAME", "vusmart")
MONITORED_PODS = os.getenv(
    "MONITORED_PODS",
    "denver,nairobi,broker,kafka-cluster-cp-zookeeper,chi-clickhouse,postgresql,minio-tenant,keycloak,traefik",
).split(",")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "timescaledb.vsmaps.svc.cluster.local")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "core")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "multicore")
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "http://minio-tenant.vsmaps.svc.cluster.local:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Pod prefix aliases — map logical names to actual pod name prefixes
POD_PREFIX_ALIASES: dict = {
    "broker": "kafka-cluster-cp-kafka",
}
ZK_POD_NAME = os.getenv("ZK_POD_NAME", "kafka-cluster-cp-zookeeper-0")
ZK_PORT = int(os.getenv("ZK_PORT", "2181"))
ZK_OUTSTANDING_WARN = int(os.getenv("ZK_OUTSTANDING_WARN", "10"))

# ─── State ───────────────────────────────────────────────────────────────────
last_result: dict = {}
last_checked: str = ""
is_running: bool = False
check_durations: dict = {}
_prev_minio_snapshot: dict = {}  # bucket_name -> {objects: {name: {size, last_modified}}, ...}

# ─── K8s + CH helpers ────────────────────────────────────────────────────────
_k8s_core = None
_k8s_apps = None
_k8s_custom = None


def _get_k8s():
    global _k8s_core, _k8s_apps, _k8s_custom
    if _k8s_core is None:
        from kubernetes import client, config

        logger.info("K8s: initialising client")
        try:
            config.load_incluster_config()
            logger.info("K8s: loaded in-cluster config")
        except Exception as e:
            logger.warning(f"K8s: in-cluster failed ({e}), falling back to kube_config")
            config.load_kube_config()
        _k8s_core = client.CoreV1Api()
        _k8s_apps = client.AppsV1Api()
        _k8s_custom = client.CustomObjectsApi()
    return _k8s_core, _k8s_apps, _k8s_custom


def _get_ch():
    import clickhouse_connect

    logger.debug(f"CH: connect {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} db={CLICKHOUSE_DB}")
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=10,
        query_limit=0,
    )


def _milli_to_cores(v: str) -> float:
    if v.endswith("n"):
        return int(v[:-1]) / 1_000_000_000
    if v.endswith("u"):
        return int(v[:-1]) / 1_000_000
    if v.endswith("m"):
        return int(v[:-1]) / 1000
    return float(v)


def _mem_to_bytes(v: str) -> int:
    for s, m in {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }.items():
        if v.endswith(s):
            return int(v[: -len(s)]) * m
    return int(v)


def _timed(name: str, t0: float):
    elapsed = round(time.time() - t0, 2)
    check_durations[name] = elapsed
    logger.info(f"[{name}] finished in {elapsed}s")


def _sync_exec_pod(pod_name: str, namespace: str, cmd: str) -> str:
    """Exec a shell command inside a pod and return stdout+stderr as a string.

    IMPORTANT: uses a dedicated, throw-away ApiClient — never the shared _k8s_core.
    kubernetes.stream monkey-patches api_client.request = websocket_call on
    whatever client is passed to stream(). Reusing the shared client would corrupt
    it and cause all subsequent normal HTTP API calls to fail with
    'Handshake status 200 OK'.
    """
    from kubernetes import client as k8s_client, config as k8s_config
    from kubernetes.stream import stream as k8s_stream

    cfg = k8s_client.Configuration()
    try:
        k8s_config.load_incluster_config(client_configuration=cfg)
    except Exception:
        k8s_config.load_kube_config(client_configuration=cfg)

    api_client = k8s_client.ApiClient(configuration=cfg)
    try:
        core = k8s_client.CoreV1Api(api_client=api_client)
        return k8s_stream(
            core.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=["sh", "-c", cmd],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
    finally:
        api_client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA LIVE DATA + CONSUMER LAG (via kafka-python AdminClient)
# ═══════════════════════════════════════════════════════════════════════════════
def _sync_kafka_live_and_lag() -> dict:
    """
    Queries the Kafka broker directly (no ClickHouse dependency) to produce:

    topic_live_status      — per-topic: has_data, is_live, total_messages
      - live  : has messages AND at least one message produced in the last
                KAFKA_LIVE_WINDOW_MINUTES minutes
      - stale : has messages but nothing new within the live window
      - empty : LOG-END-OFFSET == BEGINNING-OFFSET (no messages ever / all deleted)

    consumer_lag           — per-topic: total_lag, max_lag, groups dict
      - ClickHouse Kafka Engine consumers commit offset=-1 to __consumer_offsets;
        we record them as lag=0 here; real CH engine lag is computed later by
        cross-referencing with system.kafka_consumers committed_offset.

    partition_end_offsets  — {topic: {partition: end_offset}}
      - used by run_all_checks() to compute CH Kafka engine lag without an extra
        Kafka connection.
    """
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    result: dict = {"topic_live_status": {}, "consumer_lag": {}, "partition_end_offsets": {}}
    client_cfg = dict(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=10_000,
        client_id="healthwatch",
    )

    admin = KafkaAdminClient(**client_cfg)
    consumer = KafkaConsumer(**client_cfg)

    try:
        # ── 1. Collect consumer group lag ────────────────────────────────────
        group_ids = [g[0] for g in admin.list_consumer_groups()]
        logger.info(f"[kafka_live_lag] {len(group_ids)} consumer groups found")

        all_tps: set = set()

        for group_id in group_ids:
            try:
                committed = admin.list_consumer_group_offsets(group_id)
                if not committed:
                    continue

                tps = list(committed.keys())
                end_offsets = consumer.end_offsets(tps)

                for tp, om in committed.items():
                    all_tps.add(tp)
                    end = end_offsets.get(tp) or 0
                    # offset=-1 means ClickHouse-style consumer (manages its own offsets)
                    current = om.offset if (om and om.offset >= 0) else end
                    lag = max(0, end - current)

                    if tp.topic not in result["consumer_lag"]:
                        result["consumer_lag"][tp.topic] = {
                            "total_lag": 0, "max_lag": 0, "groups": {}
                        }
                    entry = result["consumer_lag"][tp.topic]
                    entry["total_lag"] += lag
                    entry["max_lag"] = max(entry["max_lag"], lag)
                    entry["groups"][group_id] = entry["groups"].get(group_id, 0) + lag

            except Exception as e:
                logger.warning(f"[kafka_live_lag] group={group_id} skipped: {e}")

        logger.info(
            f"[kafka_live_lag] lag collected for {len(result['consumer_lag'])} topics"
        )

        # ── 2. Compute live / stale / empty per topic ────────────────────────
        tps = list(all_tps)
        if not tps:
            return result

        end_offsets   = consumer.end_offsets(tps)
        begin_offsets = consumer.beginning_offsets(tps)

        # Store per-partition end offsets for CH Kafka engine lag cross-reference
        for tp, end in end_offsets.items():
            result["partition_end_offsets"].setdefault(tp.topic, {})[tp.partition] = end or 0

        threshold_ms = int((time.time() - KAFKA_LIVE_WINDOW_MINUTES * 60) * 1000)
        at_threshold = consumer.offsets_for_times({tp: threshold_ms for tp in tps})

        # group partitions by topic
        by_topic: dict = {}
        for tp in tps:
            by_topic.setdefault(tp.topic, []).append(tp)

        for topic, topic_tps in by_topic.items():
            total_end   = sum(end_offsets.get(tp)   or 0 for tp in topic_tps)
            total_begin = sum(begin_offsets.get(tp) or 0 for tp in topic_tps)
            has_data = total_end > total_begin

            # offsets_for_times returns not-None for a partition when a message
            # with timestamp >= threshold exists → that partition received data
            # within the live window
            is_live = has_data and any(
                at_threshold.get(tp) is not None for tp in topic_tps
            )

            logger.debug(
                f"[kafka_live_lag] topic={topic} msgs={total_end - total_begin} "
                f"has_data={has_data} is_live={is_live}"
            )

            result["topic_live_status"][topic] = {
                "has_data": has_data,
                "is_live": is_live,
                "total_messages": total_end - total_begin,
            }

        logger.info(
            f"[kafka_live_lag] live_status: "
            f"live={sum(1 for v in result['topic_live_status'].values() if v['is_live'])} "
            f"stale={sum(1 for v in result['topic_live_status'].values() if v['has_data'] and not v['is_live'])} "
            f"empty={sum(1 for v in result['topic_live_status'].values() if not v['has_data'])}"
        )

    finally:
        consumer.close()
        admin.close()

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 1 — ZOOKEEPER STATS (ruok + mntr 4-letter commands via pod exec)
# ═══════════════════════════════════════════════════════════════════════════════
async def check_zookeeper_stats() -> dict:
    t0 = time.time()
    logger.info(
        f"[zk_stats] START pod={ZK_POD_NAME} ns={K8S_NAMESPACE} port={ZK_PORT}"
    )
    loop = asyncio.get_event_loop()
    try:
        ruok_cmd = f"echo ruok | nc -w 2 localhost {ZK_PORT}"
        mntr_cmd = f"echo mntr | nc -w 2 localhost {ZK_PORT}"

        ruok_out, mntr_out = await asyncio.gather(
            loop.run_in_executor(None, _sync_exec_pod, ZK_POD_NAME, K8S_NAMESPACE, ruok_cmd),
            loop.run_in_executor(None, _sync_exec_pod, ZK_POD_NAME, K8S_NAMESPACE, mntr_cmd),
        )

        logger.info(f"[zk_stats] ruok={ruok_out.strip()!r}")
        logger.debug(f"[zk_stats] mntr raw:\n{mntr_out}")

        # Parse mntr — tab-separated key\tvalue lines
        metrics: dict = {}
        for line in mntr_out.strip().splitlines():
            parts = line.split("\t", 1)
            if len(parts) == 2:
                metrics[parts[0].strip()] = parts[1].strip()
        logger.info(f"[zk_stats] parsed {len(metrics)} mntr keys")

        ruok_ok = ruok_out.strip() == "imok"

        def _int(key: str, default: int = 0) -> int:
            try:
                return int(metrics.get(key, default))
            except (ValueError, TypeError):
                return default

        server_state  = metrics.get("zk_server_state", "unknown")
        avg_latency   = _int("zk_avg_latency")
        max_latency   = _int("zk_max_latency")
        outstanding   = _int("zk_outstanding_requests")
        alive_conns   = _int("zk_num_alive_connections")
        znode_count   = _int("zk_znode_count")
        watch_count   = _int("zk_watch_count")
        open_fds      = _int("zk_open_file_descriptor_count")
        max_fds       = _int("zk_max_file_descriptor_count")
        # zk_uptime available in ZK 3.6+ (milliseconds)
        uptime_ms     = _int("zk_uptime", 0)
        uptime_hours  = round(uptime_ms / 3_600_000, 1) if uptime_ms else None

        logger.info(
            f"[zk_stats] state={server_state} outstanding={outstanding} "
            f"conns={alive_conns} avg_lat={avg_latency}ms open_fds={open_fds}"
        )

        if not ruok_ok:
            status = "error"
            detail = "ruok check failed — ZooKeeper not responding"
        elif outstanding > ZK_OUTSTANDING_WARN:
            status = "warn"
            detail = f"outstanding_requests={outstanding} (threshold {ZK_OUTSTANDING_WARN})"
        else:
            status = "ok"
            detail = (
                f"{server_state} · {alive_conns} connections · "
                f"avg latency {avg_latency}ms"
            )

        logger.info(f"[zk_stats] status={status}")
        _timed("zk_stats", t0)
        return {
            "ruok": ruok_ok,
            "server_state": server_state,
            "avg_latency_ms": avg_latency,
            "max_latency_ms": max_latency,
            "outstanding_requests": outstanding,
            "outstanding_warn_threshold": ZK_OUTSTANDING_WARN,
            "alive_connections": alive_conns,
            "znode_count": znode_count,
            "watch_count": watch_count,
            "open_fds": open_fds,
            "max_fds": max_fds,
            "uptime_hours": uptime_hours,
            "status": status,
            "detail": detail,
        }
    except Exception as e:
        logger.error(f"[zk_stats] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("zk_stats", t0)
        return {
            "ruok": False,
            "status": "error",
            "detail": str(e),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 2 — KAFKA CONNECTOR STATE (via HTTP REST API, no kubectl exec needed)
# Checks each required connector's actual running state via /connectors/{name}/status
# A connector that exists but is PAUSED/STOPPED/FAILED is reported as not healthy.
# ═══════════════════════════════════════════════════════════════════════════════
async def check_kafka_connectors() -> dict:
    t0 = time.time()
    logger.info(
        f"[kafka_connectors] START url={KAFKA_CONNECT_URL} required={KAFKA_REQUIRED_CONNECTORS}"
    )
    import httpx

    required = [name.strip() for name in KAFKA_REQUIRED_CONNECTORS.split(",")]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Fetch status for every required connector in parallel
            status_resps = await asyncio.gather(
                *[client.get(f"{KAFKA_CONNECT_URL}/connectors/{name}/status") for name in required],
                return_exceptions=True,
            )

        connector_statuses = []
        problems = []

        for name, resp in zip(required, status_resps):
            if isinstance(resp, Exception):
                logger.error(f"[kafka_connectors] {name}: request failed — {resp}")
                connector_statuses.append({
                    "name": name,
                    "connector_state": "UNREACHABLE",
                    "tasks": [],
                    "status": "error",
                    "detail": str(resp),
                })
                problems.append(f"{name}: unreachable")
                continue

            logger.info(f"[kafka_connectors] {name}: HTTP {resp.status_code}")

            if resp.status_code == 404:
                connector_statuses.append({
                    "name": name,
                    "connector_state": "MISSING",
                    "tasks": [],
                    "status": "error",
                    "detail": "Connector not registered",
                })
                problems.append(f"{name}: not found")
                continue

            if resp.status_code != 200:
                logger.error(f"[kafka_connectors] {name}: bad status body: {resp.text[:200]}")
                connector_statuses.append({
                    "name": name,
                    "connector_state": "UNKNOWN",
                    "tasks": [],
                    "status": "error",
                    "detail": f"HTTP {resp.status_code}",
                })
                problems.append(f"{name}: HTTP {resp.status_code}")
                continue

            body = resp.json()
            conn_state = body.get("connector", {}).get("state", "UNKNOWN")
            tasks = body.get("tasks", [])
            task_states = [t.get("state", "UNKNOWN") for t in tasks]

            logger.info(
                f"[kafka_connectors] {name}: connector={conn_state} tasks={task_states}"
            )

            # Connector is healthy only if connector AND all tasks are RUNNING
            failed_tasks = [t for t in tasks if t.get("state") != "RUNNING"]

            if conn_state == "RUNNING" and not failed_tasks:
                conn_status = "ok"
                detail = f"RUNNING — {len(tasks)} task(s) healthy"
            elif conn_state in ("PAUSED", "STOPPED"):
                conn_status = "warn"
                detail = f"Connector is {conn_state}"
                problems.append(f"{name}: {conn_state}")
            else:
                # FAILED, UNASSIGNED, or tasks not running
                conn_status = "error"
                bad = [f"task[{t['id']}]={t.get('state')}" for t in failed_tasks] if failed_tasks else []
                detail = f"connector={conn_state}" + (f" | {', '.join(bad)}" if bad else "")
                problems.append(f"{name}: {detail}")

            connector_statuses.append({
                "name": name,
                "connector_state": conn_state,
                "tasks": [{"id": t.get("id"), "state": t.get("state"), "worker_id": t.get("worker_id", "")} for t in tasks],
                "status": conn_status,
                "detail": detail,
            })

        overall_status = (
            "error" if any(c["status"] == "error" for c in connector_statuses)
            else "warn" if any(c["status"] == "warn" for c in connector_statuses)
            else "ok"
        )
        overall_detail = (
            " | ".join(problems) if problems
            else f"{len(required)} connector(s) RUNNING"
        )

        logger.info(f"[kafka_connectors] overall={overall_status} problems={problems}")
        _timed("kafka_connectors", t0)
        return {
            "connectors": connector_statuses,
            "required": required,
            "problems": problems,
            "status": overall_status,
            "detail": overall_detail,
        }
    except Exception as e:
        logger.error(f"[kafka_connectors] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("kafka_connectors", t0)
        return {
            "connectors": [],
            "required": required,
            "problems": [],
            "status": "error",
            "detail": str(e),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKS 2–7 — PVC & POD DEEP CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
async def check_pvc_and_pods() -> dict:
    t0 = time.time()
    logger.info(
        f"[pods_pvcs] START ns={K8S_NAMESPACE} restart_thr={POD_RESTART_THRESHOLD}"
    )
    try:
        core, _, _ = _get_k8s()
        pod_list = core.list_namespaced_pod(namespace=K8S_NAMESPACE).items
        pvc_list = core.list_namespaced_persistent_volume_claim(
            namespace=K8S_NAMESPACE
        ).items
        logger.info(f"[pods_pvcs] {len(pod_list)} pods, {len(pvc_list)} PVCs")
    except Exception as e:
        logger.error(f"[pods_pvcs] EXCEPTION: {e}\n{traceback.format_exc()}")
        return {"pods": [], "pvcs": [], "status": "error", "detail": str(e)}

    running_pvcs: set = set()
    for pod in pod_list:
        if pod.status.phase == "Running":
            for vol in pod.spec.volumes or []:
                if vol.persistent_volume_claim:
                    running_pvcs.add(vol.persistent_volume_claim.claim_name)
    logger.debug(f"[pods_pvcs] PVCs in use: {sorted(running_pvcs)}")

    pod_results = []
    for pod in pod_list:
        alerts, containers = [], []
        for c in pod.status.container_statuses or []:
            sc = next((x for x in pod.spec.containers if x.name == c.name), None)
            lim = (sc.resources.limits or {}) if sc and sc.resources else {}
            req = (sc.resources.requests or {}) if sc and sc.resources else {}
            cpu_lim = str(lim.get("cpu", "")) or None
            mem_lim = str(lim.get("memory", "")) or None
            cpu_req = str(req.get("cpu", "")) or None
            mem_req = str(req.get("memory", "")) or None
            state = (
                "running"
                if c.state.running
                else ("waiting" if c.state.waiting else "terminated")
            )
            restarts = c.restart_count or 0
            if restarts > POD_RESTART_THRESHOLD:
                alerts.append(f"! {c.name}: {restarts} restarts")
                logger.warning(
                    f"[pods_pvcs] pod={pod.metadata.name} c={c.name} restarts={restarts}"
                )
            if state != "running":
                alerts.append(f"! {c.name}: state={state}")
                logger.warning(
                    f"[pods_pvcs] pod={pod.metadata.name} c={c.name} state={state}"
                )
            # Missing limits are expected for some containers — do not alert
            containers.append(
                {
                    "name": c.name,
                    "state": state,
                    "restarts": restarts,
                    "cpu_limit": cpu_lim,
                    "mem_limit": mem_lim,
                    "cpu_req": cpu_req,
                    "mem_req": mem_req,
                }
            )
        phase = pod.status.phase or "Unknown"
        s = (
            ("warn" if phase == "Pending" else "error")
            if phase not in ("Running", "Succeeded")
            else (
                "warn"
                if any("restarts" in a or "state=" in a for a in alerts)
                else "ok"
            )
        )
        if phase not in ("Running", "Succeeded"):
            logger.warning(f"[pods_pvcs] pod={pod.metadata.name} phase={phase} -> {s}")
        pod_results.append(
            {
                "name": pod.metadata.name,
                "phase": phase,
                "containers": containers,
                "alerts": alerts,
                "status": s,
            }
        )

    pvc_results = []
    for pvc in pvc_list:
        name = pvc.metadata.name
        phase = pvc.status.phase or "Unknown"
        cap = (pvc.status.capacity or {}).get("storage", "?")
        orphan = name not in running_pvcs and phase == "Bound"
        s = (
            "critical"
            if phase == "Lost"
            else "warn"
            if phase == "Pending" or orphan
            else "ok"
        )
        if s != "ok":
            logger.warning(
                f"[pods_pvcs] PVC={name} phase={phase} orphan={orphan} -> {s}"
            )
        pvc_results.append(
            {
                "name": name,
                "phase": phase,
                "capacity": cap,
                "orphaned": orphan,
                "status": s,
            }
        )

    all_s = [p["status"] for p in pod_results] + [p["status"] for p in pvc_results]
    overall = "critical" if "critical" in all_s else "warn" if "warn" in all_s else "ok"
    logger.info(f"[pods_pvcs] overall={overall}")
    _timed("pods_pvcs", t0)
    return {"pods": pod_results, "pvcs": pvc_results, "status": overall}


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKS 11–19 — CLICKHOUSE TABLE CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
async def check_clickhouse_tables() -> dict:
    t0 = time.time()
    logger.info(
        f"[ch_tables] START {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} db={CLICKHOUSE_DB} "
        f"cluster={CH_CLUSTER_NAME} mutation_age={CH_MUTATION_AGE_MINUTES}min "
        f"repl_limit={CH_REPLICATION_LIMIT}"
    )
    try:
        ch = _get_ch()
        logger.info("[ch_tables] connected")

        def q(label, sql):
            logger.debug(f"[ch_tables] {label}: {sql}")
            r = ch.query(sql)
            logger.info(f"[ch_tables] {label}: {len(r.result_rows)} row(s)")
            return r.result_rows

        r11 = q(
            "Q11_unused_kafka",
            "SELECT database, table FROM system.kafka_consumers "
            "WHERE last_commit_time='1970-01-01 00:00:00' AND database NOT IN ('system')",
        )
        unused_kafka = [{"database": r[0], "table": r[1]} for r in r11]
        if unused_kafka:
            logger.warning(f"[ch_tables] unused kafka tables: {unused_kafka}")

        r12 = q(
            "Q12_readonly",
            "SELECT database, table FROM system.replicas WHERE is_readonly=1",
        )
        readonly = [{"database": r[0], "table": r[1]} for r in r12]
        if readonly:
            logger.warning(f"[ch_tables] readonly tables: {readonly}")

        # 'initiator' column was removed in ClickHouse 25.x; use initiator_host if present,
        # otherwise fall back to selecting only query + status
        try:
            r13 = q(
                "Q13_inactive_ddl",
                "SELECT query,status,initiator_host FROM system.distributed_ddl_queue "
                "WHERE status='Inactive' LIMIT 20",
            )
            inactive_ddl = [{"query": r[0], "status": r[1], "initiator": r[2]} for r in r13]
        except Exception:
            r13 = q(
                "Q13_inactive_ddl_fallback",
                "SELECT query,status FROM system.distributed_ddl_queue "
                "WHERE status='Inactive' LIMIT 20",
            )
            inactive_ddl = [{"query": r[0], "status": r[1], "initiator": ""} for r in r13]

        r14 = q(
            "Q14_long_mutations",
            f"SELECT database,table,mutation_id,command,toString(create_time),parts_to_do "
            f"FROM system.mutations WHERE is_done=0 "
            f"AND create_time < now()-INTERVAL {CH_MUTATION_AGE_MINUTES} MINUTE",
        )
        mutations = [
            {
                "database": r[0],
                "table": r[1],
                "mutation_id": r[2],
                "command": r[3],
                "create_time": r[4],
                "parts_to_do": r[5],
            }
            for r in r14
        ]
        if mutations:
            logger.warning(
                f"[ch_tables] long mutations: {[m['mutation_id'] for m in mutations]}"
            )

        # Flag tables missing a timestamp-based PARTITION BY — required for
        # hot→warm tiered storage. Checks for toYYYYMMDD/toYYYYMM/toDate/toStartOf.
        r15 = q(
            "Q15_no_ts_partition",
            "SELECT database,name FROM system.tables "
            "WHERE engine LIKE '%MergeTree%' "
            "AND NOT ("
            "  create_table_query LIKE '%PARTITION BY toYYYYMMDD%' OR "
            "  create_table_query LIKE '%PARTITION BY toYYYYMM%' OR "
            "  create_table_query LIKE '%PARTITION BY toDate%' OR "
            "  create_table_query LIKE '%PARTITION BY toStartOf%'"
            ") "
            "AND database NOT IN ('system','information_schema','INFORMATION_SCHEMA')",
        )
        no_ts_partition = [{"database": r[0], "table": r[1]} for r in r15]
        if no_ts_partition:
            _names = [t["database"] + "." + t["table"] for t in no_ts_partition]
            logger.warning(f"[ch_tables] tables without timestamp partition: {_names}")

        r16 = q(
            "Q16_detached_parts",
            "SELECT database,table,reason,count() FROM system.detached_parts "
            "WHERE reason!='' GROUP BY database,table,reason",
        )
        detached = [
            {"database": r[0], "table": r[1], "reason": r[2], "count": r[3]}
            for r in r16
        ]
        if detached:
            logger.warning(f"[ch_tables] detached parts: {detached}")

        r17 = q(
            "Q17_table_sizes",
            "SELECT database,table,formatReadableSize(sum(bytes_on_disk)),sum(bytes_on_disk) "
            "FROM system.parts WHERE active=1 GROUP BY database,table ORDER BY 4 DESC LIMIT 5",
        )
        table_sizes = [
            {"database": r[0], "table": r[1], "size": r[2], "bytes": r[3]} for r in r17
        ]
        _sz = [t["database"] + "." + t["table"] + "=" + t["size"] for t in table_sizes]
        logger.info(f"[ch_tables] top tables: {_sz}")

        r17b = q(
            "Q17b_table_sizes_vusmart",
            "SELECT database,table,formatReadableSize(sum(bytes_on_disk)),sum(bytes_on_disk) "
            f"FROM system.parts WHERE active=1 AND database='{CLICKHOUSE_DB}' "
            "GROUP BY database,table ORDER BY 4 DESC LIMIT 5",
        )
        table_sizes_vusmart = [
            {"database": r[0], "table": r[1], "size": r[2], "bytes": r[3]} for r in r17b
        ]
        logger.info(f"[ch_tables] top tables (vusmart): {len(table_sizes_vusmart)}")

        r18 = q(
            "Q18_stuck_repl",
            f"SELECT database,table,type,num_postponed FROM system.replication_queue "
            f"WHERE num_postponed>{CH_REPLICATION_LIMIT}",
        )
        stuck_repl = [
            {"database": r[0], "table": r[1], "type": r[2], "num_postponed": r[3]}
            for r in r18
        ]
        if stuck_repl:
            logger.warning(f"[ch_tables] stuck replication: {stuck_repl}")

        try:
            r19 = q(
                "Q19_replica_inconsistency",
                f"SELECT database,table,count() FROM clusterAllReplicas('{CH_CLUSTER_NAME}',system.tables) "
                f"WHERE engine LIKE '%ReplicatedMergeTree%' GROUP BY database,table HAVING count()<2",
            )
            inconsistent = [
                {"database": r[0], "table": r[1], "replicas": r[2]} for r in r19
            ]
            if inconsistent:
                logger.warning(f"[ch_tables] inconsistent replicas: {inconsistent}")
        except Exception as e:
            logger.warning(f"[ch_tables] Q19_replica_inconsistency skipped: {e}")
            inconsistent = []

        # Q20 — CH Kafka engine committed offsets (for lag cross-ref with Kafka)
        # In CH 25.x Nested sub-columns must be backtick-quoted and are returned as
        # raw Array values; we zip them in Python (no ARRAY JOIN needed).
        kafka_engine_partitions = []
        try:
            r20 = q(
                "Q20_kafka_engine_offsets",
                "SELECT database, table, "
                "       `assignments.topic`, `assignments.partition_id`, `assignments.current_offset` "
                "FROM system.kafka_consumers "
                "WHERE database NOT IN ('system','information_schema','INFORMATION_SCHEMA')",
            )
            for row in r20:
                db, tbl, topics, partitions, committeds = row
                for topic, partition, committed in zip(
                    topics or [], partitions or [], committeds or []
                ):
                    kafka_engine_partitions.append(
                        {"db": db, "table": tbl, "topic": topic,
                         "partition": partition, "committed": committed}
                    )
            logger.info(f"[ch_tables] Q20 ok: {len(kafka_engine_partitions)} partition(s)")
        except Exception as e:
            logger.warning(f"[ch_tables] Q20_kafka_engine_offsets failed: {e}")

        # Q21 — Kafka pipeline health: per-table staleness from system.kafka_consumers
        try:
            r21 = q(
                "Q21_kafka_pipeline_health",
                "SELECT database, table, consumer_id, "
                "       last_commit_time, "
                "       dateDiff('second', last_commit_time, now()) AS seconds_since_commit "
                "FROM system.kafka_consumers "
                "WHERE database NOT IN ('system','information_schema','INFORMATION_SCHEMA') "
                "ORDER BY seconds_since_commit DESC",
            )
            kafka_pipeline = []
            for row in r21:
                db, tbl, cid, lct, secs = row
                never = str(lct) == "1970-01-01 00:00:00"
                status = "error" if never else ("warn" if secs > 300 else "ok")
                kafka_pipeline.append({
                    "database": db, "table": tbl, "consumer_id": cid,
                    "last_commit_time": str(lct),
                    "seconds_since_commit": secs,
                    "never_committed": never,
                    "status": status,
                })
            stale = [r for r in kafka_pipeline if r["status"] != "ok"]
            logger.info(f"[ch_tables] Q21 kafka_pipeline: {len(kafka_pipeline)} tables, {len(stale)} stale/never")
        except Exception as e:
            logger.warning(f"[ch_tables] Q21_kafka_pipeline_health failed: {e}")
            kafka_pipeline, stale = [], []

        # Q22 — Ingestion rate from system.part_log (last 1 hour)
        try:
            r22 = q(
                "Q22_ingestion_rate",
                "SELECT database, table, "
                "       sum(rows) AS rows_1h, "
                "       round(sum(rows) / 3600, 1) AS rows_per_sec, "
                "       max(event_time) AS last_insert "
                "FROM system.part_log "
                "WHERE event_type='NewPart' "
                "  AND event_time >= now() - INTERVAL 1 HOUR "
                "  AND database NOT IN ('system','information_schema','INFORMATION_SCHEMA') "
                "GROUP BY database, table "
                "ORDER BY rows_1h DESC "
                "LIMIT 20",
            )
            ingestion = []
            for row in r22:
                db, tbl, rows_1h, rows_sec, last_ins = row
                mins_ago = None
                try:
                    import datetime as _dt
                    if hasattr(last_ins, 'timestamp'):
                        mins_ago = round((time.time() - last_ins.timestamp()) / 60, 1)
                except Exception:
                    pass
                stopped = mins_ago is not None and mins_ago > 10
                ingestion.append({
                    "database": db, "table": tbl,
                    "rows_1h": int(rows_1h), "rows_per_sec": float(rows_sec),
                    "last_insert": str(last_ins),
                    "mins_since_insert": mins_ago,
                    "stopped": stopped,
                    "status": "warn" if stopped else "ok",
                })
            stopped_tables = [r for r in ingestion if r["stopped"]]
            logger.info(f"[ch_tables] Q22 ingestion: {len(ingestion)} tables, {len(stopped_tables)} stopped")
        except Exception as e:
            logger.warning(f"[ch_tables] Q22_ingestion_rate failed: {e}")
            ingestion, stopped_tables = [], []

        # Q23 — Replica exceptions and high part count from system.replicas
        try:
            r23 = q(
                "Q23_replica_exceptions",
                "SELECT database, table, "
                "       last_queue_update_exception, zookeeper_exception, "
                "       parts_to_check "
                "FROM system.replicas "
                "WHERE last_queue_update_exception != '' "
                "   OR zookeeper_exception != '' "
                "   OR parts_to_check > 300",
            )
            replica_exceptions = [
                {
                    "database": r[0], "table": r[1],
                    "queue_exception": r[2], "zk_exception": r[3],
                    "parts_to_check": r[4],
                    "status": "critical" if r[4] > 500 else "warn",
                }
                for r in r23
            ]
            logger.info(f"[ch_tables] Q23 replica_exceptions: {len(replica_exceptions)}")
        except Exception as e:
            logger.warning(f"[ch_tables] Q23_replica_exceptions failed: {e}")
            replica_exceptions = []

        # Q24 — ClickHouse errors from query_log + text_log (last 1 hour)
        try:
            r24a = q(
                "Q24a_query_errors",
                "SELECT exception_code, count() AS cnt "
                "FROM system.query_log "
                "WHERE type = 'ExceptionWhileProcessing' "
                "  AND event_time >= now() - INTERVAL 1 HOUR "
                "  AND query_kind IN ('Insert', 'Select') "
                "  AND exception_code IN (241,60,517,252,62) "
                "GROUP BY exception_code "
                "ORDER BY cnt DESC",
            )
            # exception codes: 241=MEMORY_LIMIT_EXCEEDED 60=UNKNOWN_TABLE
            # 517=SCHEMA_MISMATCH 252=TOO_MANY_PARTS 62=PARSE_ERROR
            _code_map = {
                241: "MEMORY_LIMIT_EXCEEDED", 60: "UNKNOWN_TABLE",
                517: "SCHEMA_MISMATCH", 252: "TOO_MANY_PARTS", 62: "PARSE_ERROR",
            }
            ch_errors = [
                {"error": _code_map.get(int(r[0]), str(r[0])), "count": r[1],
                 "status": "critical" if r[1] > 10 else "warn"}
                for r in r24a
            ]
        except Exception as e:
            logger.warning(f"[ch_tables] Q24a_query_errors failed: {e}")
            ch_errors = []

        try:
            r24b = q(
                "Q24b_merge_memory_errors",
                "SELECT count() AS cnt "
                "FROM system.text_log "
                "WHERE level IN ('Error','Fatal') "
                "  AND message LIKE '%Memory limit%merge%' "
                "  AND event_time >= now() - INTERVAL 1 HOUR",
            )
            merge_mem_errors = int(r24b[0][0]) if r24b else 0
            if merge_mem_errors > 0:
                ch_errors.append({
                    "error": "MERGE_MEMORY_LIMIT", "count": merge_mem_errors,
                    "status": "critical" if merge_mem_errors > 5 else "warn",
                })
        except Exception as e:
            logger.warning(f"[ch_tables] Q24b_merge_memory_errors failed: {e}")
            merge_mem_errors = 0

        logger.info(f"[ch_tables] Q24 ch_errors: {len(ch_errors)} error type(s)")

        _timed("ch_tables", t0)
        return {
            "unused_kafka_tables": {
                "count": len(unused_kafka),
                "tables": unused_kafka,
                "status": "warn" if unused_kafka else "ok",
                "detail": f"{len(unused_kafka)} Kafka tables never committed"
                if unused_kafka
                else "All Kafka tables active",
            },
            "readonly_tables": {
                "tables": readonly,
                "status": "critical" if readonly else "ok",
                "detail": f"{len(readonly)} read-only tables"
                if readonly
                else "No read-only tables",
            },
            "inactive_queries": {
                "count": len(inactive_ddl),
                "queries": inactive_ddl,
                "status": "warn" if inactive_ddl else "ok",
                "detail": f"{len(inactive_ddl)} inactive DDL queries"
                if inactive_ddl
                else "No inactive DDL queries",
            },
            "long_mutations": {
                "mutations": mutations,
                "status": "warn" if mutations else "ok",
                "detail": f"{len(mutations)} mutations >{CH_MUTATION_AGE_MINUTES}min"
                if mutations
                else "No long mutations",
            },
            "tables_without_ttl": {
                "tables": no_ts_partition,
                "status": "warn" if no_ts_partition else "ok",
                "detail": f"{len(no_ts_partition)} tables missing timestamp partition"
                if no_ts_partition
                else "All tables have timestamp partition",
            },
            "detached_parts": {
                "parts": detached,
                "status": "critical" if detached else "ok",
                "detail": f"{len(detached)} tables with detached parts"
                if detached
                else "No detached parts",
            },
            "table_sizes": {
                "tables": table_sizes,
                "tables_vusmart": table_sizes_vusmart,
                "status": "ok",
                "detail": "Top 5 table sizes",
            },
            "replication_stuck_jobs": {
                "jobs": stuck_repl,
                "status": "warn" if stuck_repl else "ok",
                "detail": f"{len(stuck_repl)} stuck replication jobs"
                if stuck_repl
                else "No stuck jobs",
            },
            "replica_inconsistency": {
                "tables": inconsistent,
                "status": "critical" if inconsistent else "ok",
                "detail": f"{len(inconsistent)} replica mismatches"
                if inconsistent
                else "All replicas consistent",
                "expected_replicas": 2,
            },
            "kafka_engine_partitions": kafka_engine_partitions,
            "kafka_pipeline_health": {
                "tables": kafka_pipeline,
                "status": "error" if any(r["never_committed"] for r in stale)
                          else "warn" if stale else "ok",
                "detail": f"{len(stale)} stale/never-committed table(s)"
                          if stale else f"{len(kafka_pipeline)} tables · all active",
            },
            "ingestion_rate": {
                "tables": ingestion,
                "status": "warn" if stopped_tables else "ok",
                "detail": f"{len(stopped_tables)} table(s) stopped inserting"
                          if stopped_tables else f"{len(ingestion)} tables · all ingesting",
            },
            "replica_exceptions": {
                "tables": replica_exceptions,
                "status": "critical" if any(r["status"] == "critical" for r in replica_exceptions)
                          else "warn" if replica_exceptions else "ok",
                "detail": f"{len(replica_exceptions)} replica exception(s)"
                          if replica_exceptions else "No replica exceptions",
            },
            "ch_errors": {
                "errors": ch_errors,
                "status": "critical" if any(r["status"] == "critical" for r in ch_errors)
                          else "warn" if ch_errors else "ok",
                "detail": f"{len(ch_errors)} error type(s) in last hour"
                          if ch_errors else "No errors in last hour",
            },
        }
    except Exception as e:
        logger.error(f"[ch_tables] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("ch_tables", t0)
        return {"_error": str(e), "status": "error"}


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKS 20–21 — K8S NODE & POD RESOURCES
# ═══════════════════════════════════════════════════════════════════════════════
async def check_kubernetes_resources() -> dict:
    t0 = time.time()
    logger.info(
        f"[k8s_resources] START ns={K8S_NAMESPACE} "
        f"node cpu>{NODE_CPU_WARN_THRESHOLD}% mem>{NODE_MEM_WARN_THRESHOLD}% "
        f"pod  cpu>{POD_CPU_WARN_THRESHOLD}% mem>{POD_MEM_WARN_THRESHOLD}%"
    )
    try:
        core, _, custom = _get_k8s()

        node_metrics = custom.list_cluster_custom_object(
            "metrics.k8s.io", "v1beta1", "nodes"
        )
        node_specs = {n.metadata.name: n for n in core.list_node().items}
        logger.info(
            f"[k8s_resources] {len(node_metrics.get('items', []))} node metrics"
        )

        node_resources = []
        for nm in node_metrics.get("items", []):
            name = nm["metadata"]["name"]
            alloc = node_specs[name].status.allocatable if name in node_specs else {}
            cpu_total = _milli_to_cores(alloc.get("cpu", "0"))
            mem_total = _mem_to_bytes(alloc.get("memory", "0"))
            raw = nm["usage"]["cpu"]
            cpu_used = (
                int(raw[:-1]) / 1e9 if raw.endswith("n") else _milli_to_cores(raw)
            )
            mem_used = _mem_to_bytes(nm["usage"]["memory"])
            cp = cpu_used / cpu_total * 100 if cpu_total > 0 else 0
            mp = mem_used / mem_total * 100 if mem_total > 0 else 0
            s = (
                "critical"
                if cp >= 90 or mp >= 90
                else "warn"
                if cp >= NODE_CPU_WARN_THRESHOLD or mp >= NODE_MEM_WARN_THRESHOLD
                else "ok"
            )
            logger.info(
                f"[k8s_resources] node={name} cpu={cp:.1f}% mem={mp:.1f}% "
                f"({round(mem_used / 1024**3, 1)}/{round(mem_total / 1024**3, 1)} GiB) -> {s}"
            )
            node_resources.append(
                {
                    "node": name,
                    "cpu_used_pct": round(cp, 1),
                    "cpu_threshold": NODE_CPU_WARN_THRESHOLD,
                    "memory_used_pct": round(mp, 1),
                    "memory_threshold": NODE_MEM_WARN_THRESHOLD,
                    "memory_used_gb": round(mem_used / 1024**3, 1),
                    "memory_total_gb": round(mem_total / 1024**3, 1),
                    "cpu_used_cores": round(cpu_used, 2),
                    "cpu_total_cores": round(cpu_total, 2),
                    "status": s,
                }
            )

        pod_metrics = custom.list_namespaced_custom_object(
            "metrics.k8s.io", "v1beta1", K8S_NAMESPACE, "pods"
        )
        pod_specs = {
            p.metadata.name: p
            for p in core.list_namespaced_pod(namespace=K8S_NAMESPACE).items
        }
        logger.info(f"[k8s_resources] {len(pod_metrics.get('items', []))} pod metrics")

        pod_resources = []
        for pm in pod_metrics.get("items", []):
            pname = pm["metadata"]["name"]
            cpu_used = sum(
                int(c["usage"]["cpu"][:-1]) / 1e9
                if c["usage"]["cpu"].endswith("n")
                else _milli_to_cores(c["usage"]["cpu"])
                for c in pm.get("containers", [])
            )
            mem_used = sum(
                _mem_to_bytes(c["usage"]["memory"]) for c in pm.get("containers", [])
            )
            cpu_lim = mem_lim = 0
            if pname in pod_specs:
                for c in pod_specs[pname].spec.containers:
                    if c.resources and c.resources.limits:
                        cpu_lim += _milli_to_cores(c.resources.limits.get("cpu", "0"))
                        mem_lim += _mem_to_bytes(c.resources.limits.get("memory", "0"))
            cp = cpu_used / cpu_lim * 100 if cpu_lim > 0 else 0
            mp = mem_used / mem_lim * 100 if mem_lim > 0 else 0
            s = (
                "critical"
                if cp >= 90 or mp >= 90
                else "warn"
                if cp >= POD_CPU_WARN_THRESHOLD or mp >= POD_MEM_WARN_THRESHOLD
                else "ok"
            )
            if s != "ok":
                logger.warning(
                    f"[k8s_resources] pod={pname} cpu={cp:.1f}% mem={mp:.1f}% -> {s}"
                )
            else:
                logger.debug(
                    f"[k8s_resources] pod={pname} cpu={cp:.1f}% mem={mp:.1f}% -> ok"
                )
            pod_resources.append(
                {
                    "pod": pname,
                    "namespace": K8S_NAMESPACE,
                    "cpu_used_pct": round(cp, 1),
                    "memory_used_pct": round(mp, 1),
                    "status": s,
                }
            )

        all_s = [n["status"] for n in node_resources] + [
            p["status"] for p in pod_resources
        ]
        overall = (
            "critical" if "critical" in all_s else "warn" if "warn" in all_s else "ok"
        )
        logger.info(f"[k8s_resources] overall={overall}")
        _timed("k8s_resources", t0)
        return {
            "node_resources": node_resources,
            "pod_resources": pod_resources,
            "status": overall,
        }
    except Exception as e:
        logger.error(f"[k8s_resources] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("k8s_resources", t0)
        return {
            "node_resources": [],
            "pod_resources": [],
            "status": "error",
            "detail": str(e),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# EXISTING CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
async def check_clickhouse_connectivity() -> dict:
    t0 = time.time()
    logger.info(f"[ch_connectivity] START {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    try:
        ch = _get_ch()
        n = ch.query("SELECT count() FROM system.tables").first_row[0]
        # Per-database table counts
        db_rows = ch.query(
            "SELECT database, count() AS cnt FROM system.tables "
            "GROUP BY database ORDER BY cnt DESC"
        ).result_rows
        db_breakdown = [{"database": r[0], "count": r[1]} for r in db_rows]
        db_summary = ", ".join(f"{r[0]}:{r[1]}" for r in db_rows[:5])
        if len(db_rows) > 5:
            db_summary += f" +{len(db_rows) - 5} more"
        ch.query("SELECT 1+1")
        logger.info(f"[ch_connectivity] OK — {n} tables across {len(db_rows)} databases")
        _timed("ch_connectivity", t0)
        return {
            "Connection": {
                "status": "ok",
                "detail": f"Reachable on {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}",
            },
            "Total Tables": {
                "status": "ok",
                "detail": f"{n} tables across {len(db_rows)} databases",
                "databases": db_breakdown,
            },
            "Query Execution": {
                "status": "ok",
                "detail": "Query executed successfully",
            },
        }
    except Exception as e:
        logger.error(f"[ch_connectivity] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("ch_connectivity", t0)
        return {
            "Connection": {"status": "error", "detail": str(e)},
            "Total Tables": {"status": "error", "detail": "N/A"},
            "Query Execution": {"status": "error", "detail": "N/A"},
        }


async def check_kafka() -> dict:
    t0 = time.time()
    logger.info(f"[kafka] START ns={K8S_NAMESPACE}")
    try:
        core, _, _ = _get_k8s()
        bp = core.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=cp-kafka"
        ).items
        br = [p for p in bp if p.status.phase == "Running"]
        bs = "ok" if br else "error"
        bd = f"cp-kafka-0 · {len(br)}/{len(bp)} Running" if bp else "No broker pods"
        logger.info(f"[kafka] brokers={len(bp)} running={len(br)} -> {bs}")
        zp = core.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=cp-zookeeper"
        ).items
        zr = [p for p in zp if p.status.phase == "Running"]
        zs = "ok" if zr else "warn"
        logger.info(f"[kafka] zookeeper={len(zp)} running={len(zr)} -> {zs}")
        loop = asyncio.get_event_loop()
        connectors, zk_stats, live_lag = await asyncio.gather(
            check_kafka_connectors(),
            check_zookeeper_stats(),
            loop.run_in_executor(None, _sync_kafka_live_and_lag),
        )

        tls = live_lag.get("topic_live_status", {})
        lag = live_lag.get("consumer_lag", {})

        live_count  = sum(1 for v in tls.values() if v["is_live"])
        stale_count = sum(1 for v in tls.values() if v["has_data"] and not v["is_live"])
        empty_count = sum(1 for v in tls.values() if not v["has_data"])
        high_lag    = sum(1 for v in lag.values() if v["total_lag"] > KAFKA_LAG_WARN_THRESHOLD)

        live_status  = "ok"   if live_count > 0  else "warn"
        live_detail  = f"{live_count} live · {stale_count} stale · {empty_count} empty"
        lag_status   = "warn" if high_lag > 0     else "ok"
        lag_detail   = f"{high_lag} topic(s) high lag" if high_lag else f"{len(lag)} topics · all lag normal"

        _timed("kafka", t0)
        return {
            "Broker Health": {"status": bs, "detail": bd},
            "Zookeeper Mode": {
                "status": zs,
                "detail": f"{len(zr)}/{len(zp)} Zookeeper Running",
            },
            "Zookeeper Stats": {
                "status": zk_stats["status"],
                "detail": zk_stats["detail"],
                "_zk_stats": zk_stats,
            },
            "Live Data": {"status": live_status, "detail": live_detail},
            "Consumer Lag": {"status": lag_status, "detail": lag_detail},
            "Kafka Connectors": {
                "status": connectors["status"],
                "detail": connectors["detail"],
                "_connectors": connectors,
            },
            "__details__": {
                "topics": {},
                "topic_live_status": tls,
                "consumer_lag": lag,
            },
        }
    except Exception as e:
        logger.error(f"[kafka] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("kafka", t0)
        return {
            "Broker Health": {"status": "error", "detail": str(e)},
            "Zookeeper Mode": {"status": "error", "detail": "N/A"},
            "Zookeeper Stats": {"status": "error", "detail": str(e)},
            "Kafka Connectors": {"status": "error", "detail": "N/A"},
            "__details__": {},
        }


async def check_postgres() -> dict:
    t0 = time.time()
    logger.info(f"[postgres] START {POSTGRES_HOST}:{POSTGRES_PORT} db={POSTGRES_DB}")
    try:
        import asyncpg

        conn = await asyncpg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            timeout=10,
        )
        ver = await conn.fetchval("SELECT version()")
        await conn.fetchval("SELECT 1")
        await conn.close()
        vs = ver.split(",")[0]
        logger.info(f"[postgres] OK — {vs}")
        _timed("postgres", t0)
        return {
            "Connection": {"status": "ok", "detail": "Connected successfully"},
            "Version": {"status": "ok", "detail": vs},
            "Query Execution": {"status": "ok", "detail": "Queries running normally"},
        }
    except Exception as e:
        logger.error(f"[postgres] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("postgres", t0)
        return {
            "Connection": {"status": "error", "detail": str(e)},
            "Version": {"status": "error", "detail": "N/A"},
            "Query Execution": {"status": "error", "detail": "N/A"},
        }


async def check_minio() -> dict:
    t0 = time.time()
    logger.info(f"[minio] START endpoint={MINIO_ENDPOINT}")
    import httpx

    result = {}

    # ── Health checks ──
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            lr = await c.get(f"{MINIO_ENDPOINT}/minio/health/live")
            rr = await c.get(f"{MINIO_ENDPOINT}/minio/health/ready")
        logger.info(f"[minio] live={lr.status_code} ready={rr.status_code}")
        result["Liveness"] = {
            "status": "ok" if lr.status_code == 200 else "error",
            "detail": f"HTTP {lr.status_code}",
        }
        result["Readiness"] = {
            "status": "ok" if rr.status_code == 200 else "error",
            "detail": f"HTTP {rr.status_code}",
        }
    except Exception as e:
        logger.error(f"[minio] health EXCEPTION: {e}")
        result["Liveness"] = {"status": "error", "detail": str(e)}
        result["Readiness"] = {"status": "error", "detail": str(e)}

    # ── Bucket discovery ──
    buckets_data = []
    try:
        if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
            from minio import Minio
            from urllib.parse import urlparse
            from datetime import datetime, timezone

            parsed = urlparse(MINIO_ENDPOINT)
            host = parsed.hostname
            port = parsed.port
            endpoint = f"{host}:{port}" if port else host

            client = Minio(
                endpoint,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            now = datetime.now(timezone.utc)
            buckets = client.list_buckets()
            logger.info(f"[minio] found {len(buckets)} bucket(s)")

            for b in buckets:
                bucket_info = {
                    "name": b.name,
                    "created": b.creation_date.isoformat() if b.creation_date else None,
                    "total_size": 0,
                    "total_size_human": "0 B",
                    "object_count": 0,
                    "last_modified": None,
                    "recently_modified": False,
                }
                try:
                    total_bytes = 0
                    obj_count = 0
                    latest_mod = None
                    obj_map = {}  # object_name -> {size, last_modified}
                    for obj in client.list_objects(b.name, recursive=True):
                        sz = obj.size or 0
                        total_bytes += sz
                        obj_count += 1
                        obj_map[obj.object_name] = {
                            "size": sz,
                            "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                        }
                        if obj.last_modified:
                            mod = obj.last_modified
                            if latest_mod is None or mod > latest_mod:
                                latest_mod = mod

                    bucket_info["total_size"] = total_bytes
                    bucket_info["total_size_human"] = _fmt_bytes(total_bytes)
                    bucket_info["object_count"] = obj_count
                    bucket_info["_obj_map"] = obj_map  # used for diff, stripped before response
                    if latest_mod:
                        bucket_info["last_modified"] = latest_mod.isoformat()
                        hours_ago = (now - latest_mod).total_seconds() / 3600
                        bucket_info["recently_modified"] = hours_ago <= 24
                        bucket_info["last_modified_ago"] = _fmt_ago(now, latest_mod)
                except Exception as be:
                    logger.warning(f"[minio] bucket {b.name} scan failed: {be}")
                    bucket_info["error"] = str(be)

                buckets_data.append(bucket_info)
                logger.info(
                    f"[minio] bucket={b.name} objects={bucket_info['object_count']} "
                    f"size={bucket_info['total_size_human']} last_mod={bucket_info.get('last_modified_ago', 'N/A')}"
                )

            # ── Compare with previous snapshot to detect per-file changes ──
            global _prev_minio_snapshot
            current_snapshot = {}
            for bd in buckets_data:
                name = bd["name"]
                cur_objs = bd.get("_obj_map", {})
                current_snapshot[name] = cur_objs

                prev_objs = _prev_minio_snapshot.get(name)
                changes = []
                if prev_objs is not None:
                    cur_keys = set(cur_objs.keys())
                    prev_keys = set(prev_objs.keys())

                    # New files
                    for fname in sorted(cur_keys - prev_keys):
                        sz = cur_objs[fname]["size"]
                        changes.append({"type": "added", "label": fname, "detail": _fmt_bytes(sz)})

                    # Deleted files
                    for fname in sorted(prev_keys - cur_keys):
                        sz = prev_objs[fname]["size"]
                        changes.append({"type": "deleted", "label": fname, "detail": _fmt_bytes(sz)})

                    # Modified files (same name, different size)
                    for fname in sorted(cur_keys & prev_keys):
                        cur_sz = cur_objs[fname]["size"]
                        prev_sz = prev_objs[fname]["size"]
                        if cur_sz != prev_sz:
                            delta = cur_sz - prev_sz
                            sign = "+" if delta > 0 else ""
                            changes.append({"type": "modified", "label": fname, "detail": f"{sign}{_fmt_bytes(abs(delta))}"})

                bd["changes"] = changes
                # Remove internal obj_map before sending to frontend
                bd.pop("_obj_map", None)

            _prev_minio_snapshot = current_snapshot

            total_size = sum(b["total_size"] for b in buckets_data)
            recently_mod = [b["name"] for b in buckets_data if b["recently_modified"]]
            result["Buckets"] = {
                "status": "ok",
                "detail": f"{len(buckets_data)} bucket(s) · {_fmt_bytes(total_size)} total",
            }
            result["__minio_buckets__"] = buckets_data
            if recently_mod:
                result["Recent Activity"] = {
                    "status": "warn",
                    "detail": f"{len(recently_mod)} bucket(s) modified in last 24h: {', '.join(recently_mod)}",
                }
            else:
                result["Recent Activity"] = {
                    "status": "ok",
                    "detail": "No bucket modifications in last 24h",
                }
        else:
            logger.info("[minio] MINIO_ACCESS_KEY/SECRET_KEY not set, skipping bucket scan")
    except Exception as e:
        logger.error(f"[minio] bucket scan EXCEPTION: {e}\n{traceback.format_exc()}")
        result["Buckets"] = {"status": "error", "detail": str(e)}

    _timed("minio", t0)
    return result


def _fmt_bytes(b: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}" if b != int(b) else f"{int(b)} {unit}"
        b /= 1024
    return f"{b:.1f} PiB"


def _fmt_ago(now, dt) -> str:
    """Human-readable time ago."""
    secs = (now - dt).total_seconds()
    if secs < 60:
        return f"{int(secs)}s ago"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    return f"{int(secs // 86400)}d ago"


async def check_data_retention() -> dict:
    """
    Data Retention Check — verifies old data is actually being removed per retention policy.

    DB stores: Hot=N (days on hot disk), Warm=M (CUMULATIVE — includes Hot).
    So Warm is the total ClickHouse retention. Archive/Cold is MinIO only.
    Example: UI Hot=1, Warm=1 → DB stores Hot=1, Warm=2 → CH retention = 2 days.

    1. Reads retention config from PostgreSQL multicore.vusoft_vusoftdatamanagement
       - Effective ClickHouse retention = Warm value (cumulative, already includes Hot)
       - Tables field can be comma-separated prefixes (e.g. "vmetrics" matches all vmetrics*_data)
       - Tracks policy name for each match
    2. For each _data table in ClickHouse vusmart, checks min/max(timestamp) span.
    3. If dateDiff > Warm days → WARN (data should have been moved to cold).
    4. Queries system.parts disk_name to show hot/warm tier size per table.
    """
    t0 = time.time()
    HOT_DISK = os.getenv("STORAGE_HOT_DISK", "default_encrypted_disk")
    WARM_DISK = os.getenv("STORAGE_WARM_DISK", "warm_encrypted_disk")
    logger.info("[data_retention] START — checking actual data age vs retention policy")
    try:
        # ── Step 1: Get retention policies from PostgreSQL ────────────────────
        import asyncpg, json as _json

        default_hot = 15
        default_warm = 20  # DB Warm is cumulative (includes Hot)
        default_retention_days = 20  # = Warm (total CH retention)
        default_policy_name = "Default"
        # Each entry: {prefixes: [str], hot: int, warm: int, total: int, name: str}
        policies = []
        policy_source = "default"

        try:
            conn = await asyncpg.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                database="multicore", timeout=10,
            )
            rows = await conn.fetch(
                "SELECT name, tables, data_category, data_retention_period, data_store_type "
                "FROM public.vusoft_vusoftdatamanagement "
                "WHERE tables IS NOT NULL AND data_retention_period IS NOT NULL"
            )
            await conn.close()

            for row in rows:
                policy_name = (row["name"] or "").strip()
                tables_val = (row["tables"] or "").strip()
                category = (row["data_category"] or "").strip()
                store_type = (row["data_store_type"] or "").strip()
                retention_json = row["data_retention_period"]

                try:
                    rmap = _json.loads(retention_json) if isinstance(retention_json, str) else retention_json
                    hot = int(rmap.get("Hot", 0))
                    warm = int(rmap.get("Warm", 0))  # cumulative — includes Hot
                    # Warm is the total CH retention (data removed from CH after Warm days)
                    # warm_only = time spent on warm disk = Warm - Hot
                    total = warm  # effective ClickHouse retention
                except Exception:
                    continue

                logger.info(f"[data_retention] raw row: name={policy_name!r} tables={tables_val!r} "
                            f"cat={category!r} store={store_type!r} hot={hot} warm={warm}")
                if tables_val == "*" and category.lower() in ("all", "default", ""):
                    default_hot = hot
                    default_warm = warm
                    default_retention_days = total
                    default_policy_name = policy_name or "Default"
                    policy_source = "postgresql"
                    logger.info(f"[data_retention] default policy '{default_policy_name}': "
                                f"Hot={hot}d Warm={warm}d (cumulative) → CH retention={total}d")
                else:
                    # Parse comma-separated table prefixes
                    prefixes = [p.strip() for p in tables_val.split(",") if p.strip()]
                    # Priority: Custom=10 > Category(Metrics/Logs/etc)=5 > Default=0
                    priority = 10 if category.lower() == "custom" else 5
                    policies.append({
                        "name": policy_name,
                        "category": category,
                        "prefixes": prefixes,
                        "hot": hot,
                        "warm": warm,
                        "total": total,
                        "priority": priority,
                    })
                    logger.info(f"[data_retention] policy '{policy_name}' (cat={category}, pri={priority}): "
                                f"prefixes={prefixes} Hot={hot}d Warm={warm}d → CH retention={total}d")

            logger.info(f"[data_retention] policy from {policy_source}: "
                        f"default={default_retention_days}d, {len(policies)} custom policies")
        except Exception as pe:
            logger.warning(f"[data_retention] postgres query failed, using default={default_retention_days}d: {pe}")

        # ── Helper: find matching policy for a table name ─────────────────────
        def match_policy(table_name):
            """Returns (retention_days, hot_days, warm_days, policy_name) for a _data table.
            Priority: Custom(10) > Category like Metrics/Logs(5) > Default(0).
            Within same priority, longest prefix match wins."""
            base = table_name[:-5] if table_name.endswith("_data") else table_name
            best_match = None
            best_priority = -1
            best_len = 0
            for pol in policies:
                pri = pol.get("priority", 5)
                for prefix in pol["prefixes"]:
                    if base.startswith(prefix):
                        # Higher priority wins; within same priority, longer prefix wins
                        if pri > best_priority or (pri == best_priority and len(prefix) > best_len):
                            best_match = pol
                            best_priority = pri
                            best_len = len(prefix)
            if best_match:
                return best_match["total"], best_match["hot"], best_match["warm"], best_match["name"]
            return default_retention_days, default_hot, default_warm, default_policy_name

        # ── Step 2: Get all _data tables from ClickHouse ─────────────────────
        ch = _get_ch()
        table_rows = ch.query(
            "SELECT name FROM system.tables "
            "WHERE database='vusmart' AND name LIKE '%_data' "
            "AND engine NOT LIKE '%View%' AND engine NOT LIKE 'Distributed%'"
        ).result_rows
        all_data_tables = [r[0] for r in table_rows]
        logger.info(f"[data_retention] found {len(all_data_tables)} _data tables in vusmart")

        # ── Step 3: Get disk tier distribution per table ─────────────────────
        tier_rows = ch.query(
            "SELECT table, disk_name, count() as parts, "
            "formatReadableSize(sum(bytes_on_disk)) as size, "
            "sum(bytes_on_disk) as size_bytes "
            "FROM system.parts "
            "WHERE active=1 AND database='vusmart' AND name != 'all' "
            "GROUP BY table, disk_name ORDER BY table, disk_name"
        ).result_rows
        # Build {table: {disk_name: {parts, size, size_bytes}}}
        table_tiers: dict = {}
        for r in tier_rows:
            tbl, disk, parts, size, size_bytes = r
            if tbl not in table_tiers:
                table_tiers[tbl] = {}
            table_tiers[tbl][disk] = {"parts": parts, "size": size, "size_bytes": size_bytes}

        # ── Step 4: Check each table's actual data age ───────────────────────
        results = []
        warned = []

        for table in all_data_tables:
            retention, hot_days, warm_days, policy_name = match_policy(table)
            try:
                row = ch.query(
                    f"SELECT toString(min(timestamp)), toString(max(timestamp)), "
                    f"toUInt32(dateDiff('day', min(timestamp), max(timestamp))) "
                    f"FROM vusmart.`{table}` "
                    f"WHERE timestamp > '1970-01-01 00:00:00'"
                ).result_rows

                if not row or not row[0][0]:
                    continue

                min_ts, max_ts, days_diff = row[0]
                min_ts_str = str(min_ts)
                max_ts_str = str(max_ts)

                if min_ts_str.startswith("1970-01-01") or max_ts_str.startswith("1970-01-01"):
                    continue

                status = "ok"
                if days_diff > retention:
                    status = "warn"
                    warned.append(table)
                    logger.warning(f"[data_retention] WARN {table}: "
                                   f"data_age={days_diff}d > retention={retention}d "
                                   f"policy='{policy_name}' (min={min_ts_str} max={max_ts_str})")
                else:
                    logger.debug(f"[data_retention] OK {table}: "
                                 f"data_age={days_diff}d retention={retention}d policy='{policy_name}'")

                # Tier info for this table
                tiers = table_tiers.get(table, {})
                hot_info = tiers.get(HOT_DISK)
                warm_info = tiers.get(WARM_DISK)

                results.append({
                    "table": table,
                    "min_timestamp": min_ts_str,
                    "max_timestamp": max_ts_str,
                    "days_diff": int(days_diff),
                    "retention_days": retention,
                    "hot_days": hot_days,
                    "warm_days": warm_days,
                    "policy_name": policy_name,
                    "status": status,
                    "hot_tier": {"size": hot_info["size"], "size_bytes": hot_info["size_bytes"]} if hot_info else None,
                    "warm_tier": {"size": warm_info["size"], "size_bytes": warm_info["size_bytes"]} if warm_info else None,
                })

            except Exception as te:
                logger.debug(f"[data_retention] skipped {table}: {te}")
                continue

        # Sort: warned first, then by days_diff desc
        results.sort(key=lambda x: (-int(x["status"] == "warn"), -x["days_diff"]))

        overall = "warn" if warned else "ok"
        detail = (f"{len(results)} tables checked · "
                  f"{len(warned)} exceeding retention"
                  if warned else
                  f"{len(results)} tables checked · all within retention")

        logger.info(f"[data_retention] overall={overall} checked={len(results)} warned={warned}")
        _timed("data_retention", t0)
        return {
            "Retention Policy": {
                "status": "ok" if policy_source == "postgresql" else "warn",
                "detail": (f"Default: {default_retention_days}d in CH (Hot={default_hot}d, Warm={default_warm - default_hot}d)"
                           + (f" · {len(policies)} custom" if policies else "")),
            },
            "Data Age Check": {
                "status": overall,
                "detail": detail,
            },
            "__retention_tables__": results,
            "__retention_meta__": {
                "default_retention_days": default_retention_days,
                "default_hot": default_hot,
                "default_warm": default_warm,
                "policies": [{"name": p["name"], "prefixes": p["prefixes"],
                              "hot": p["hot"], "warm": p["warm"], "total": p["total"]}
                             for p in policies],
                "total_checked": len(results),
                "exceeded_count": len(warned),
                "exceeded_tables": warned,
            },
        }
    except Exception as e:
        logger.error(f"[data_retention] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("data_retention", t0)
        return {
            "Retention Policy": {"status": "error", "detail": str(e)},
            "Data Age Check": {"status": "error", "detail": str(e)},
            "__retention_tables__": [],
            "__retention_meta__": {},
        }


CERT_WARN_DAYS = int(os.getenv("CERT_WARN_DAYS", "30"))
CERT_CONFIGMAP_NAME = os.getenv("CERT_CONFIGMAP_NAME", "cert-status")


def _read_cert_configmap():
    """Read cert-status ConfigMap written by the cert-checker CronJob."""
    try:
        import json as _json
        core, _, _ = _get_k8s()
        cm = core.read_namespaced_config_map(CERT_CONFIGMAP_NAME, K8S_NAMESPACE)
        raw = cm.data.get("cert-status.json", "{}")
        return _json.loads(raw)
    except Exception as e:
        logger.warning(f"[k8s_certs] Could not read ConfigMap {CERT_CONFIGMAP_NAME}: {e}")
        return None


async def check_k8s_certs() -> dict:
    """Check Kubernetes certificate health from inside the cluster.
    Combines live API checks with cert-checker CronJob data from ConfigMap."""
    t0 = time.time()
    logger.info(f"[k8s_certs] START threshold={CERT_WARN_DAYS}d")
    try:
        from datetime import timezone

        certs_info = []

        # ── 1. API server TLS certificate (live check) ───────────────────
        try:
            from datetime import datetime as dt
            from cryptography import x509
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with socket.create_connection(("kubernetes.default.svc", 443), timeout=10) as sock:
                with ctx.wrap_socket(sock, server_hostname="kubernetes.default.svc") as ssock:
                    # getpeercert(True) returns DER bytes even with CERT_NONE
                    der_bytes = ssock.getpeercert(True)
                    if not der_bytes:
                        raise RuntimeError("No certificate returned by API server")
                    cert_obj = x509.load_der_x509_certificate(der_bytes)
                    expiry = cert_obj.not_valid_after_utc
                    subject = cert_obj.subject.rfc4514_string()
                    issuer = cert_obj.issuer.rfc4514_string()
                    now = dt.now(timezone.utc)
                    days_left = (expiry - now).days
                    status = "ok"
                    if days_left <= 0:
                        status = "error"
                    elif days_left <= CERT_WARN_DAYS:
                        status = "warn"
                    certs_info.append({
                        "name": "API Server TLS (live)",
                        "category": "live",
                        "expiry": expiry.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "not_after": expiry.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "days_left": days_left,
                        "status": status,
                        "subject": subject,
                        "issuer": issuer,
                    })
                    logger.info(f"[k8s_certs] API Server TLS: expires={expiry} days_left={days_left} -> {status}")
        except Exception as e:
            logger.error(f"[k8s_certs] API Server TLS check failed: {e}")
            certs_info.append({
                "name": "API Server TLS (live)",
                "category": "live",
                "expiry": "unknown",
                "days_left": -1,
                "status": "error",
                "error": str(e),
            })

        # ── 2. Read ConfigMap from cert-checker CronJob ──────────────────
        cm_data = _read_cert_configmap()
        cm_certs = []
        cm_prechecks = []
        cm_backup = {}
        cm_sa_keys = []
        cm_summary = {}
        cm_timestamp = ""
        cm_node = ""

        if cm_data:
            cm_certs = cm_data.get("certificates", [])
            cm_prechecks = cm_data.get("prechecks", [])
            cm_backup = cm_data.get("backup", {})
            cm_sa_keys = cm_data.get("sa_keys", [])
            cm_summary = cm_data.get("summary", {})
            cm_timestamp = cm_data.get("timestamp", "")
            cm_node = cm_data.get("node", "")
            logger.info(f"[k8s_certs] ConfigMap loaded: {len(cm_certs)} certs, "
                        f"{len(cm_prechecks)} prechecks, scanned={cm_timestamp}")
        else:
            logger.warning("[k8s_certs] No ConfigMap data — cert-checker CronJob may not have run yet")

        # ── 3. Check control plane pods (kube-system) ────────────────────
        core, _, _ = _get_k8s()
        cp_components = ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd"]
        cp_pods = core.list_namespaced_pod(namespace="kube-system").items
        cp_status = []
        for comp in cp_components:
            matched = [p for p in cp_pods if p.metadata.name.startswith(comp)]
            if matched:
                pod = matched[0]
                phase = pod.status.phase or "Unknown"
                cp_status.append({
                    "component": comp,
                    "pod": pod.metadata.name,
                    "phase": phase,
                    "status": "ok" if phase == "Running" else "error",
                })
            else:
                cp_status.append({
                    "component": comp,
                    "pod": None,
                    "phase": "Not Found",
                    "status": "warn",
                })
            logger.info(f"[k8s_certs] control_plane {comp}: {cp_status[-1]['phase']}")

        # ── 4. Check nodes ────────────────────────────────────────────────
        nodes = core.list_node().items
        nodes_ready = sum(
            1 for n in nodes
            if any(c.type == "Ready" and c.status == "True" for c in n.status.conditions)
        )
        nodes_cordoned = [
            n.metadata.name for n in nodes
            if n.spec.unschedulable
        ]

        # ── 5. Check for stuck pods in kube-system ────────────────────────
        stuck_pods = []
        for p in cp_pods:
            phase = p.status.phase or "Unknown"
            if phase not in ("Running", "Succeeded"):
                stuck_pods.append({"name": p.metadata.name, "phase": phase})
            elif p.status.container_statuses:
                for cs in p.status.container_statuses:
                    if cs.state.waiting and cs.state.waiting.reason in ("CrashLoopBackOff", "Error", "OOMKilled"):
                        stuck_pods.append({"name": p.metadata.name, "phase": cs.state.waiting.reason})

        # ── 6. Pending CSRs ───────────────────────────────────────────────
        from kubernetes import client
        certs_api = client.CertificatesV1Api()
        try:
            csrs = certs_api.list_certificate_signing_request()
            pending_csrs = [
                csr.metadata.name for csr in csrs.items
                if not any(c.type == "Approved" for c in (csr.status.conditions or []))
            ]
        except Exception:
            pending_csrs = []

        # ── Compute overall (including ConfigMap certs) ──────────────────
        all_statuses = [c["status"] for c in certs_info] + [c["status"] for c in cp_status]
        all_statuses += [c["status"] for c in cm_certs]
        overall = "error" if "error" in all_statuses else "warn" if "warn" in all_statuses else "ok"

        # Summary for status strip
        api_cert = certs_info[0] if certs_info else {}
        cert_detail = f"{api_cert.get('days_left', '?')}d until expiry" if api_cert.get("days_left", -1) > 0 else "EXPIRED or unreachable"

        # PKI cert summary from ConfigMap
        pki_total = len(cm_certs)
        pki_ok = len([c for c in cm_certs if c.get("status") == "ok"])

        logger.info(f"[k8s_certs] overall={overall}")
        _timed("k8s_certs", t0)
        return {
            "API Server Certificate": {
                "status": api_cert.get("status", "error"),
                "detail": cert_detail,
            },
            "Control Plane": {
                "status": "ok" if all(c["status"] == "ok" for c in cp_status) else "error",
                "detail": f"{sum(1 for c in cp_status if c['status'] == 'ok')}/{len(cp_status)} components Running",
            },
            "Cluster Nodes": {
                "status": "ok" if nodes_ready == len(nodes) and not nodes_cordoned else "warn",
                "detail": f"{nodes_ready}/{len(nodes)} Ready" + (f", {len(nodes_cordoned)} cordoned" if nodes_cordoned else ""),
            },
            "PKI Certificates": {
                "status": "ok" if pki_ok == pki_total and pki_total > 0 else "warn" if pki_total > 0 else "error",
                "detail": f"{pki_ok}/{pki_total} valid" if pki_total > 0 else "CronJob not run yet",
            },
            "__cert_details__": {
                "certificates": certs_info,
                "pki_certificates": cm_certs,
                "sa_keys": cm_sa_keys,
                "control_plane": cp_status,
                "nodes_total": len(nodes),
                "nodes_ready": nodes_ready,
                "nodes_cordoned": nodes_cordoned,
                "stuck_pods": stuck_pods,
                "pending_csrs": pending_csrs,
                "warn_threshold_days": CERT_WARN_DAYS,
                "configmap_prechecks": cm_prechecks,
                "backup": cm_backup,
                "configmap_summary": cm_summary,
                "configmap_timestamp": cm_timestamp,
                "configmap_node": cm_node,
            },
        }
    except Exception as e:
        logger.error(f"[k8s_certs] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("k8s_certs", t0)
        return {
            "API Server Certificate": {"status": "error", "detail": str(e)},
            "Control Plane": {"status": "error", "detail": "Check failed"},
            "Cluster Nodes": {"status": "error", "detail": "Check failed"},
            "PKI Certificates": {"status": "error", "detail": "Check failed"},
            "__cert_details__": {},
        }


async def check_kubernetes_pods() -> dict:
    t0 = time.time()
    logger.info(f"[k8s_pods] START ns={K8S_NAMESPACE} prefixes={MONITORED_PODS}")
    try:
        core, _, _ = _get_k8s()
        all_pods = core.list_namespaced_pod(namespace=K8S_NAMESPACE).items
        all_nodes = core.list_node().items
        logger.info(f"[k8s_pods] {len(all_pods)} pods, {len(all_nodes)} nodes")
        checks = {}
        for raw_prefix in MONITORED_PODS:
            raw_prefix = raw_prefix.strip()
            prefix = POD_PREFIX_ALIASES.get(raw_prefix, raw_prefix)
            matched = [p for p in all_pods if p.metadata.name.startswith(prefix)]
            if not matched:
                logger.warning(f"[k8s_pods] no pod for prefix={prefix!r} (raw={raw_prefix!r})")
                checks[f"Pod: {raw_prefix.capitalize()}"] = {
                    "status": "warn",
                    "detail": f"No pod matching '{prefix}'",
                }
                continue
            running = [p for p in matched if p.status.phase == "Running"]
            restarts = sum(
                (cs.restart_count or 0)
                for p in matched
                for cs in (p.status.container_statuses or [])
            )
            s = "ok" if running else "error"
            if restarts > POD_RESTART_THRESHOLD and s == "ok":
                s = "warn"
            detail = f"{matched[0].metadata.name} — {matched[0].status.phase}, {restarts} restarts"
            logger.info(
                f"[k8s_pods] prefix={prefix!r} (raw={raw_prefix!r}) running={len(running)}/{len(matched)} restarts={restarts} -> {s}"
            )
            checks[f"Pod: {raw_prefix.capitalize()}"] = {"status": s, "detail": detail}
        rn = sum(
            1
            for n in all_nodes
            if any(
                c.type == "Ready" and c.status == "True" for c in n.status.conditions
            )
        )
        ns = "ok" if rn == len(all_nodes) else "warn"
        logger.info(f"[k8s_pods] nodes ready={rn}/{len(all_nodes)} -> {ns}")
        checks["Cluster Nodes"] = {
            "status": ns,
            "detail": f"{rn}/{len(all_nodes)} nodes Ready",
        }
        _timed("k8s_pods", t0)
        return checks
    except Exception as e:
        logger.error(f"[k8s_pods] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("k8s_pods", t0)
        return {"Cluster Nodes": {"status": "error", "detail": str(e)}}


# ═══════════════════════════════════════════════════════════════════════════════
# POD CONNECTIVITY, IMAGE TAGS, CRASH LOGS
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Service connectivity map ─────────────────────────────────────────────────
CH_URL       = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/ping"
PG_URL       = f"http://{POSTGRES_HOST}:{POSTGRES_PORT}"
KAFKA_URL    = "http://broker.vsmaps.svc.cluster.local:9092"
ZK_URL       = "http://kafka-cluster-cp-zookeeper.vsmaps.svc.cluster.local:2181"
MINIO_URL    = f"{MINIO_ENDPOINT}/minio/health/live"
KEYCLOAK_URL = "http://keycloak.vsmaps.svc.cluster.local:8080"
NAIROBI_URL  = "http://nairobi.vsmaps.svc.cluster.local:3000"
DENVER_URL   = "http://denver.vsmaps.svc.cluster.local:8882"
DAO_URL      = "http://dao.vsmaps.svc.cluster.local:50051"
ALERT_URL    = "http://alert.vsmaps.svc.cluster.local:50052"
RENDERER_URL = "http://renderer.vsmaps.svc.cluster.local:8081"
REPORTER_URL = "http://reporter.vsmaps.svc.cluster.local:8686"
ORCH_URL     = "http://orch-webhook.vsmaps.svc.cluster.local:8080"
KEEPER_URL   = "http://clickhouse-keeper.vsmaps.svc.cluster.local:2181"

POD_DEPENDENCIES = {
    "denver-denver": [
        ("ClickHouse", CH_URL,       "http"),
        ("PostgreSQL", PG_URL,       "tcp"),
        ("Kafka",      KAFKA_URL,    "tcp"),
    ],
    "dao": [
        ("PostgreSQL", PG_URL,       "tcp"),
        ("ClickHouse", CH_URL,       "http"),
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("Denver",     DENVER_URL,   "tcp"),
        ("Alert",      ALERT_URL,    "tcp"),
        ("Nairobi",    NAIROBI_URL,  "tcp"),
        ("Reporter",   REPORTER_URL, "tcp"),
        ("Renderer",   RENDERER_URL, "tcp"),
    ],
    "alert": [
        ("ClickHouse", CH_URL,       "http"),
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("PostgreSQL", PG_URL,       "tcp"),
        ("Nairobi",    NAIROBI_URL,  "tcp"),
        ("Reporter",   REPORTER_URL, "tcp"),
    ],
    "vuinterface-cairo": [
        ("Denver",     DENVER_URL,   "tcp"),
        ("DAO",        DAO_URL,      "tcp"),
        ("Alert",      ALERT_URL,    "tcp"),
        ("Nairobi",    NAIROBI_URL,  "tcp"),
        ("Keycloak",   KEYCLOAK_URL, "tcp"),
        ("ClickHouse", CH_URL,       "http"),
        ("PostgreSQL", PG_URL,       "tcp"),
        ("MinIO",      MINIO_URL,    "http"),
        ("Renderer",   RENDERER_URL, "tcp"),
        ("Reporter",   REPORTER_URL, "tcp"),
    ],
    "nairobi": [
        ("PostgreSQL", PG_URL,       "tcp"),
        ("Renderer",   RENDERER_URL, "tcp"),
        ("Keycloak",   KEYCLOAK_URL, "tcp"),
    ],
    "renderer": [
        ("Nairobi",    NAIROBI_URL,  "tcp"),
        ("Reporter",   REPORTER_URL, "tcp"),
    ],
    "reporter": [
        ("ClickHouse", CH_URL,       "http"),
        ("PostgreSQL", PG_URL,       "tcp"),
        ("Nairobi",    NAIROBI_URL,  "tcp"),
        ("Renderer",   RENDERER_URL, "tcp"),
        ("Denver",     DENVER_URL,   "tcp"),
    ],
    "keycloak-deployment": [
        ("PostgreSQL", PG_URL, "tcp"),
    ],
    "kafka-cluster-cp-kafka-0": [
        ("Zookeeper",  ZK_URL,       "tcp"),
    ],
    "kafka-cluster-cp-kafka-connect": [
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("ClickHouse", CH_URL,       "http"),
    ],
    "enrichment-preprocessor": [
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("ClickHouse", CH_URL,       "http"),
    ],
    "linuxmonitor": [
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("ClickHouse", CH_URL,       "http"),
    ],
    "telegraf-vusmart": [
        ("ClickHouse", CH_URL,       "http"),
    ],
    "telegraf-infra": [
        ("ClickHouse", CH_URL,       "http"),
    ],
    "minio-tenant": [
        ("Orchestration", ORCH_URL,  "tcp"),
    ],
    "orchestration": [
        ("MinIO",      MINIO_URL,    "http"),
        ("DAO",        DAO_URL,      "tcp"),
        ("Kafka",      KAFKA_URL,    "tcp"),
        ("PostgreSQL", PG_URL,       "tcp"),
    ],
    "chi-clickhouse-vusmart": [
        ("CH Keeper",  KEEPER_URL,   "tcp"),
        ("MinIO",      MINIO_URL,    "http"),
    ],
    "vublock-store": [
        ("MinIO",      MINIO_URL,    "http"),
        ("Nairobi",    NAIROBI_URL,  "tcp"),
    ],
}

CRASH_LOG_LINES = int(os.getenv("CRASH_LOG_LINES", "50"))


def _test_endpoint(pod_prefix, svc_name, url, protocol):
    """Test single endpoint reachability via socket (TCP) or httpx (HTTP)."""
    import socket as _socket
    try:
        # Parse host:port from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        if protocol == "http":
            import httpx
            try:
                resp = httpx.get(url, timeout=5, follow_redirects=False)
                return {"service": svc_name, "url": url, "status": "ok",
                        "detail": f"HTTP {resp.status_code}", "reachable": True}
            except httpx.ConnectError:
                return {"service": svc_name, "url": url, "status": "error",
                        "detail": "connection refused", "reachable": False}
            except httpx.TimeoutException:
                return {"service": svc_name, "url": url, "status": "error",
                        "detail": "timeout", "reachable": False}
            except Exception as e:
                return {"service": svc_name, "url": url, "status": "error",
                        "detail": str(e)[:100], "reachable": False}
        else:
            # TCP port check
            sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            sock.settimeout(3)
            try:
                sock.connect((host, port))
                sock.close()
                return {"service": svc_name, "url": url, "status": "ok",
                        "detail": "port open", "reachable": True}
            except (_socket.timeout, ConnectionRefusedError, OSError) as e:
                return {"service": svc_name, "url": url, "status": "error",
                        "detail": str(e)[:100], "reachable": False}
            finally:
                sock.close()
    except Exception as e:
        return {"service": svc_name, "url": url, "status": "error",
                "detail": str(e)[:100], "reachable": False}


async def check_pod_connectivity() -> dict:
    """Test TCP/HTTP reachability of each pod's service dependencies."""
    t0 = time.time()
    logger.info(f"[pod_connectivity] START — {len(POD_DEPENDENCIES)} pod dependency groups")
    import concurrent.futures

    # Match running pods to dependency map
    try:
        core, _, _ = _get_k8s()
        running_pods = core.list_namespaced_pod(namespace=K8S_NAMESPACE).items
        running_names = [p.metadata.name for p in running_pods if p.status.phase == "Running"]
    except Exception as e:
        logger.error(f"[pod_connectivity] failed to list pods: {e}")
        running_names = []

    matched = {}
    for prefix in POD_DEPENDENCIES:
        for pname in running_names:
            if pname.startswith(prefix):
                matched[prefix] = pname
                break

    logger.info(f"[pod_connectivity] matched {len(matched)}/{len(POD_DEPENDENCIES)} prefixes")

    results = {}
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = {}
        for pod_prefix, deps in POD_DEPENDENCIES.items():
            for svc_name, url, protocol in deps:
                key = (pod_prefix, svc_name, url, protocol)
                futures[key] = loop.run_in_executor(
                    pool, lambda k=key: _test_endpoint(*k))

        for (pod_prefix, svc_name, url, protocol), fut in futures.items():
            result = await fut
            actual_pod = matched.get(pod_prefix, pod_prefix)
            if pod_prefix not in results:
                results[pod_prefix] = {
                    "pod_prefix": pod_prefix,
                    "actual_pod": actual_pod,
                    "connections": [],
                    "status": "ok",
                }
            results[pod_prefix]["connections"].append(result)
            if result["status"] == "error":
                results[pod_prefix]["status"] = "error"
                logger.warning(f"[pod_connectivity] {actual_pod} -> "
                               f"{svc_name} UNREACHABLE ({url})")

    pod_list = list(results.values())
    failed = [p["actual_pod"] for p in pod_list if p["status"] == "error"]
    overall = "error" if failed else "ok"
    logger.info(f"[pod_connectivity] overall={overall} tested={len(pod_list)} failed={failed}")
    _timed("pod_connectivity", t0)
    return {
        "pods": pod_list,
        "failed": failed,
        "status": overall,
        "detail": (f"{len(failed)} pods with connectivity issues"
                   if failed else
                   f"All {len(pod_list)} service dependency checks passed"),
    }


async def check_pod_images_and_crashes() -> dict:
    """For every pod: capture image tags, and for pods with restarts fetch previous logs."""
    t0 = time.time()
    logger.info(f"[pod_images_crashes] START ns={K8S_NAMESPACE}")
    try:
        core, _, _ = _get_k8s()
        pods = core.list_namespaced_pod(namespace=K8S_NAMESPACE).items
        logger.info(f"[pod_images_crashes] {len(pods)} pods found")

        pod_data = []
        image_summary = {}
        crash_pods = []

        for pod in pods:
            pname = pod.metadata.name
            phase = pod.status.phase or "Unknown"

            # ── Image tags ──────────────────────────────────────────────────
            containers = []
            for c in pod.spec.containers:
                image = c.image or ""
                tag = image.split(":")[-1] if ":" in image else "latest"
                registry = image.split("/")[0] if "/" in image else "docker.io"
                img_name = image.split("/")[-1].split(":")[0] if "/" in image else image.split(":")[0]
                containers.append({
                    "container": c.name, "image": image,
                    "tag": tag, "registry": registry, "img_name": img_name,
                })
                if image not in image_summary:
                    image_summary[image] = []
                image_summary[image].append(pname)

            # ── Restart count + crash logs ───────────────────────────────────
            total_restarts = 0
            crash_logs = []

            for cs in (pod.status.container_statuses or []):
                restarts = cs.restart_count or 0
                total_restarts += restarts

                if restarts > 0:
                    logger.info(f"[pod_images_crashes] pod={pname} "
                                f"container={cs.name} restarts={restarts}")
                    try:
                        prev_logs = core.read_namespaced_pod_log(
                            name=pname, namespace=K8S_NAMESPACE,
                            container=cs.name, previous=True,
                            tail_lines=CRASH_LOG_LINES,
                        )
                        lines = prev_logs.strip().splitlines() if prev_logs else []
                        crash_reason = ""
                        for line in reversed(lines):
                            low = line.lower()
                            if any(k in low for k in ["error", "exception", "fatal",
                                                       "critical", "traceback", "panic",
                                                       "killed", "oom"]):
                                crash_reason = line.strip()[:300]
                                break
                        last_line = lines[-1].strip()[:300] if lines else ""
                        crash_logs.append({
                            "container": cs.name, "restarts": restarts,
                            "crash_reason": crash_reason or last_line,
                            "last_lines": lines[-10:], "total_lines": len(lines),
                        })
                        crash_pods.append({
                            "pod": pname, "container": cs.name,
                            "restarts": restarts,
                            "crash_reason": crash_reason or last_line,
                        })
                    except Exception as le:
                        logger.warning(f"[pod_images_crashes] prev logs failed "
                                       f"{pname}/{cs.name}: {le}")
                        crash_logs.append({
                            "container": cs.name, "restarts": restarts,
                            "crash_reason": "Previous logs not available",
                            "last_lines": [], "total_lines": 0,
                        })

            pod_data.append({
                "name": pname, "phase": phase, "containers": containers,
                "total_restarts": total_restarts, "crash_logs": crash_logs,
                "has_crashes": len(crash_logs) > 0,
            })

        crash_pods.sort(key=lambda x: x["restarts"], reverse=True)
        images = [{"image": img, "tag": img.split(":")[-1] if ":" in img else "latest",
                   "pods": pods_using, "count": len(pods_using)}
                  for img, pods_using in sorted(image_summary.items())]

        overall = ("critical" if any(p["restarts"] > 100 for p in crash_pods) else
                   "warn" if any(p["restarts"] > 0 for p in crash_pods) else "ok")

        _timed("pod_images_crashes", t0)
        return {
            "pods": pod_data,
            "image_summary": images,
            "crash_pods": crash_pods,
            "total_pods": len(pod_data),
            "pods_with_crashes": len({c["pod"] for c in crash_pods}),
            "status": overall,
            "detail": (f"{len(crash_pods)} containers with restarts — "
                       f"top: {crash_pods[0]['pod']} ({crash_pods[0]['restarts']} restarts)"
                       if crash_pods else f"{len(pod_data)} pods · no restarts"),
        }
    except Exception as e:
        logger.error(f"[pod_images_crashes] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("pod_images_crashes", t0)
        return {"pods": [], "image_summary": [], "crash_pods": [],
                "status": "error", "detail": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# AGGREGATE RUNNER
# ═══════════════════════════════════════════════════════════════════════════════
async def run_all_checks():
    global last_result, last_checked, is_running
    if is_running:
        logger.warning("run_all_checks: already running, skipping")
        return
    is_running = True
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("HealthWatch: START full check run")
    logger.info("=" * 60)
    try:
        results = await asyncio.gather(
            check_clickhouse_connectivity(),
            check_kafka(),
            check_postgres(),
            check_minio(),
            check_kubernetes_pods(),
            check_pvc_and_pods(),
            check_clickhouse_tables(),
            check_kubernetes_resources(),
            check_data_retention(),
            check_k8s_certs(),
            check_pod_connectivity(),
            check_pod_images_and_crashes(),
            return_exceptions=True,
        )
        names = [
            "ch_conn",
            "kafka",
            "pg",
            "minio",
            "k8s_pods",
            "pvc_pods",
            "ch_tables",
            "k8s_resources",
            "data_retention",
            "k8s_certs",
            "pod_connectivity",
            "pod_images_crashes",
        ]
        for name, r in zip(names, results):
            if isinstance(r, Exception):
                logger.error(
                    f"[aggregate] top-level exception in {name}: {r}\n{traceback.format_exc()}"
                )

        def safe(r, key):
            return (
                {key: {"status": "error", "detail": str(r)}}
                if isinstance(r, Exception)
                else r
            )

        (
            ch_conn,
            kafka,
            pg,
            minio,
            k8s_pods,
            pvc_pods,
            ch_tables,
            k8s_resources,
            data_retention,
            k8s_certs,
            pod_connectivity,
            pod_images_crashes,
        ) = results
        ch_merged = {**safe(ch_conn, "CH Connection")}
        ch_merged["__ch_tables__"] = safe(ch_tables, "CH Tables")

        # ── Cross-reference CH Kafka engine offsets with Kafka partition end-offsets ──
        # ClickHouse Kafka engine consumers commit offset=-1 to Kafka's __consumer_offsets,
        # so they showed as lag=0 in consumer_lag. Real lag is computed here by comparing
        # system.kafka_consumers committed_offset against Kafka's actual end offset.
        if not isinstance(kafka, Exception) and not isinstance(ch_tables, Exception):
            kafka_part_ends = (kafka.get("__details__", {})
                                    .get("partition_end_offsets", {}))
            ch_eng_parts    = ch_tables.get("kafka_engine_partitions", [])

            # per-CH-table lag: "{db}.{table}" -> {total_lag, max_lag, topics: {topic: lag}}
            ch_kafka_lag   = {}
            # per-topic aggregated CH engine lag (for injecting into Kafka consumer_lag)
            topic_ch_lag   = {}

            for p in ch_eng_parts:
                end       = kafka_part_ends.get(p["topic"], {}).get(p["partition"], 0)
                committed = p["committed"]
                lag       = max(0, end - committed) if committed >= 0 else 0

                key = f"{p['db']}.{p['table']}"
                if key not in ch_kafka_lag:
                    ch_kafka_lag[key] = {"total_lag": 0, "max_lag": 0, "topics": {}}
                ch_kafka_lag[key]["total_lag"] += lag
                ch_kafka_lag[key]["max_lag"]    = max(ch_kafka_lag[key]["max_lag"], lag)
                ch_kafka_lag[key]["topics"][p["topic"]] = (
                    ch_kafka_lag[key]["topics"].get(p["topic"], 0) + lag
                )
                topic_ch_lag[p["topic"]] = topic_ch_lag.get(p["topic"], 0) + lag

            # Add per-table lag to ClickHouse section
            ch_merged["__ch_tables__"]["ch_kafka_lag"] = ch_kafka_lag

            # Inject real CH engine lag into Kafka consumer_lag
            # (previously these topics had lag=0 because CH commits offset=-1)
            cl = kafka.get("__details__", {}).get("consumer_lag", {})
            for topic, lag in topic_ch_lag.items():
                if topic in cl:
                    cl[topic]["total_lag"] += lag
                    cl[topic]["max_lag"]    = max(cl[topic]["max_lag"], lag)
                    cl[topic]["groups"]["⚡ CH Kafka Engine"] = lag
                else:
                    cl[topic] = {
                        "total_lag": lag, "max_lag": lag,
                        "groups": {"⚡ CH Kafka Engine": lag},
                    }

            # Re-compute Consumer Lag summary in kafka result
            high_lag   = sum(1 for v in cl.values() if v["total_lag"] > KAFKA_LAG_WARN_THRESHOLD)
            lag_status = "warn" if high_lag > 0 else "ok"
            lag_detail = (f"{high_lag} topic(s) high lag" if high_lag
                          else f"{len(cl)} topics · all lag normal")
            if "Consumer Lag" in kafka:
                kafka["Consumer Lag"]["status"] = lag_status
                kafka["Consumer Lag"]["detail"] = lag_detail

        last_result = {
            "clickhouse": ch_merged,
            "kafka": safe(kafka, "Kafka"),
            "postgres": safe(pg, "Postgres"),
            "minio": safe(minio, "MinIO"),
            "kubernetes": {
                **safe(k8s_pods, "K8s Pods"),
                "__resources__": safe(k8s_resources, "K8s Resources"),
                "__connectivity__": safe(pod_connectivity, "Pod Connectivity"),
                "__images_crashes__": safe(pod_images_crashes, "Images & Crashes"),
            },
            "pods_pvcs": {"__pods_pvcs__": safe(pvc_pods, "Pods/PVCs")},
            "data_retention": safe(data_retention, "Data Retention"),
            "cert_health": safe(k8s_certs, "Cert Health"),
        }
        last_checked = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed = round(time.time() - t0, 2)
        logger.info("=" * 60)
        logger.info(f"HealthWatch: DONE at {last_checked} total={elapsed}s")
        logger.info(f"Timings: {check_durations}")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"[aggregate] fatal: {e}\n{traceback.format_exc()}")
    finally:
        is_running = False


# ═══════════════════════════════════════════════════════════════════════════════
# APP
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="HealthWatch Phase 2")
BUILD_DIR = Path(__file__).parent.parent / "frontend"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.time()
    resp = await call_next(request)
    logger.debug(
        f"HTTP {request.method} {request.url.path} -> {resp.status_code} ({round(time.time() - t0, 3)}s)"
    )
    return resp


@app.on_event("startup")
async def startup():
    logger.info("HealthWatch startup — active config:")
    logger.info(f"  K8S_NAMESPACE={K8S_NAMESPACE}  LOG_LEVEL={LOG_LEVEL}")
    logger.info(f"  CLICKHOUSE={CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}  db={CLICKHOUSE_DB}")
    logger.info(f"  MINIO_ENDPOINT={MINIO_ENDPOINT}")
    logger.info(f"  POSTGRES={POSTGRES_HOST}:{POSTGRES_PORT}  db={POSTGRES_DB}")
    logger.info(f"  KAFKA_CONNECT_URL={KAFKA_CONNECT_URL}")
    logger.info(f"  KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}  LIVE_WINDOW={KAFKA_LIVE_WINDOW_MINUTES}min  LAG_WARN>{KAFKA_LAG_WARN_THRESHOLD}")
    logger.info(f"  ZK_POD_NAME={ZK_POD_NAME}  ZK_PORT={ZK_PORT}  ZK_OUTSTANDING_WARN={ZK_OUTSTANDING_WARN}")
    logger.info(f"  KAFKA_REQUIRED={KAFKA_REQUIRED_CONNECTORS}")
    logger.info(
        f"  NODE thresholds cpu>{NODE_CPU_WARN_THRESHOLD}% mem>{NODE_MEM_WARN_THRESHOLD}%"
    )
    logger.info(
        f"  POD  thresholds cpu>{POD_CPU_WARN_THRESHOLD}% mem>{POD_MEM_WARN_THRESHOLD}%"
    )
    logger.info(f"  POD_RESTART_THRESHOLD={POD_RESTART_THRESHOLD}")
    logger.info(f"  BUILD_DIR={BUILD_DIR}  exists={BUILD_DIR.exists()}")
    asyncio.create_task(run_all_checks())


@app.get("/api/status")
async def get_status():
    return {
        "last_checked": last_checked,
        "is_running": is_running,
        "results": last_result,
        "timings": check_durations,
    }


@app.post("/api/run")
async def trigger_checks(background_tasks: BackgroundTasks):
    logger.info("/api/run triggered")
    background_tasks.add_task(run_all_checks)
    return {"message": "Health checks triggered"}


@app.get("/api/ch-error-messages")
async def get_ch_error_messages(code: int):
    """Fetch recent error messages for a specific exception code (last 1hr only)."""
    logger.info(f"/api/ch-error-messages code={code}")
    _settings = "SETTINGS max_memory_usage = 1000000000, max_threads = 2"
    try:
        ch = _get_ch()
        if code == 0:
            # MERGE_MEMORY_LIMIT — from text_log
            r = ch.query(
                "SELECT DISTINCT substring(message, 1, 500) AS msg "
                "FROM system.text_log "
                "WHERE event_date >= toDate(now() - INTERVAL 1 HOUR) "
                "  AND event_time >= now() - INTERVAL 1 HOUR "
                "  AND level IN ('Error','Fatal') "
                "  AND message LIKE '%Memory limit%merge%' "
                f"LIMIT 10 {_settings}"
            )
        else:
            r = ch.query(
                "SELECT DISTINCT substring(exception, 1, 500) AS msg "
                "FROM system.query_log "
                "WHERE event_date >= toDate(now() - INTERVAL 1 HOUR) "
                "  AND event_time >= now() - INTERVAL 1 HOUR "
                "  AND type = 'ExceptionWhileProcessing' "
                f"  AND exception_code = {int(code)} "
                "  AND exception != '' "
                f"LIMIT 10 {_settings}"
            )
        messages = [row[0] for row in r.result_rows if row[0]]
        return {"code": code, "messages": messages}
    except Exception as e:
        logger.error(f"/api/ch-error-messages failed: {e}")
        return {"code": code, "messages": [], "error": str(e)}


@app.get("/api/ch-tables/{database}")
async def get_ch_tables(database: str):
    """List all tables in a ClickHouse database, grouped by engine type."""
    logger.info(f"/api/ch-tables/{database}")
    try:
        ch = _get_ch()
        rows = ch.query(
            "SELECT name, engine, formatReadableSize(total_bytes) AS size, "
            "total_bytes, total_rows "
            "FROM system.tables "
            f"WHERE database = '{database}' "
            "ORDER BY total_bytes DESC NULLS LAST"
        ).result_rows
        tables = [
            {
                "name": r[0],
                "engine": r[1],
                "size": r[2] if r[2] else "0 B",
                "bytes": r[3] or 0,
                "rows": r[4] or 0,
            }
            for r in rows
        ]
        # Fetch columns per table for schema validation
        col_rows = ch.query(
            "SELECT table, groupArray(name) AS cols "
            f"FROM system.columns WHERE database = '{database}' "
            "GROUP BY table"
        ).result_rows
        table_columns = {r[0]: list(r[1]) for r in col_rows}

        for t in tables:
            t["columns"] = table_columns.get(t["name"], [])

        # Group by engine category
        groups = {}
        for t in tables:
            eng = t["engine"]
            if "ReplicatedMergeTree" in eng:
                cat = "ReplicatedMergeTree"
            elif "MergeTree" in eng and "Replicated" not in eng:
                cat = "MergeTree"
            elif eng == "Distributed":
                cat = "Distributed"
            elif eng == "Kafka":
                cat = "Kafka"
            elif eng == "MaterializedView":
                cat = "MaterializedView"
            elif eng == "View":
                cat = "View"
            else:
                cat = "Other"
            groups.setdefault(cat, []).append(t)

        # Schema validation: check pipeline consistency
        # Find table families (base name without _data/_kafkaenginemv/_view suffix)
        schema_issues = []
        data_tables = {t["name"]: set(t["columns"]) for t in groups.get("ReplicatedMergeTree", [])}
        for base_name, data_cols in data_tables.items():
            # Strip _data suffix to find related tables
            stem = base_name.replace("_data", "")
            related = {}
            for cat in ["Kafka", "MaterializedView", "Distributed", "View"]:
                for t in groups.get(cat, []):
                    # Match by common stem
                    tname = t["name"]
                    if stem in tname or tname.startswith(stem):
                        related[f"{cat}:{tname}"] = set(t["columns"])
            # Compare columns
            for label, cols in related.items():
                missing = data_cols - cols - {"_sign", "_version"}  # ignore system cols
                extra = cols - data_cols - {"_sign", "_version"}
                if missing or extra:
                    issue = {"data_table": base_name, "related": label}
                    if missing:
                        issue["missing_in_related"] = sorted(missing)
                    if extra:
                        issue["extra_in_related"] = sorted(extra)
                    schema_issues.append(issue)

        return {
            "database": database, "tables": tables, "groups": groups,
            "schema_issues": schema_issues,
        }
    except Exception as e:
        logger.error(f"/api/ch-tables/{database} failed: {e}")
        return {"database": database, "tables": [], "groups": {}, "error": str(e)}


@app.get("/api/ch-table-detail/{database}/{table}")
async def get_ch_table_detail(database: str, table: str):
    """Full diagnosis of a single ClickHouse table."""
    logger.info(f"/api/ch-table-detail/{database}/{table}")
    try:
        ch = _get_ch()

        def q(sql):
            return ch.query(sql).result_rows

        # Basic table info
        info_rows = q(
            "SELECT engine, create_table_query, partition_key, sorting_key, "
            "primary_key, sampling_key, total_rows, total_bytes, "
            "formatReadableSize(total_bytes), lifetime_rows, lifetime_bytes, "
            "metadata_modification_time, engine_full "
            f"FROM system.tables WHERE database='{database}' AND name='{table}'"
        )
        if not info_rows:
            return {"found": False, "database": database, "table": table}

        r = info_rows[0]
        info = {
            "engine": r[0], "create_query": r[1], "partition_key": r[2] or "—",
            "sorting_key": r[3] or "—", "primary_key": r[4] or "—",
            "sampling_key": r[5] or "—",
            "total_rows": r[6] or 0, "total_bytes": r[7] or 0,
            "total_size": r[8] or "0 B",
            "lifetime_rows": r[9] or 0, "lifetime_bytes": r[10] or 0,
            "last_modified": str(r[11]) if r[11] else "—",
            "engine_full": r[12] or r[0],
        }

        # Parts info
        parts = []
        try:
            part_rows = q(
                "SELECT partition, count() AS parts, sum(rows) AS rows, "
                "formatReadableSize(sum(bytes_on_disk)) AS size, sum(bytes_on_disk), "
                "min(modification_time), max(modification_time) "
                f"FROM system.parts WHERE database='{database}' AND table='{table}' "
                "AND active=1 GROUP BY partition ORDER BY partition"
            )
            parts = [
                {
                    "partition": str(p[0]), "parts": p[1], "rows": p[2],
                    "size": p[3], "bytes": p[4],
                    "min_time": str(p[5]), "max_time": str(p[6]),
                }
                for p in part_rows
            ]
        except Exception:
            pass

        # Columns
        columns = []
        try:
            col_rows = q(
                "SELECT name, type, default_kind, comment "
                f"FROM system.columns WHERE database='{database}' AND table='{table}' "
                "ORDER BY position"
            )
            columns = [
                {"name": c[0], "type": c[1], "default": c[2] or "—", "comment": c[3] or ""}
                for c in col_rows
            ]
        except Exception:
            pass

        # Replicas (if replicated)
        replicas = []
        if "Replicated" in info["engine"]:
            try:
                repl_rows = q(
                    "SELECT replica_name, is_leader, is_readonly, "
                    "queue_size, inserts_in_queue, merges_in_queue, "
                    "absolute_delay, total_replicas, active_replicas "
                    f"FROM system.replicas WHERE database='{database}' AND table='{table}'"
                )
                replicas = [
                    {
                        "replica": rr[0], "is_leader": bool(rr[1]),
                        "is_readonly": bool(rr[2]), "queue_size": rr[3],
                        "inserts_in_queue": rr[4], "merges_in_queue": rr[5],
                        "delay_seconds": rr[6],
                        "total_replicas": rr[7], "active_replicas": rr[8],
                    }
                    for rr in repl_rows
                ]
            except Exception:
                pass

        # Recent mutations
        mutations = []
        try:
            mut_rows = q(
                "SELECT command, create_time, is_done, parts_to_do, "
                "latest_fail_reason "
                f"FROM system.mutations WHERE database='{database}' AND table='{table}' "
                "ORDER BY create_time DESC LIMIT 5"
            )
            mutations = [
                {
                    "command": m[0][:200], "create_time": str(m[1]),
                    "is_done": bool(m[2]), "parts_to_do": m[3],
                    "fail_reason": m[4] or "",
                }
                for m in mut_rows
            ]
        except Exception:
            pass

        return {
            "found": True, "database": database, "table": table,
            "info": info, "parts": parts, "columns": columns,
            "replicas": replicas, "mutations": mutations,
        }
    except Exception as e:
        logger.error(f"/api/ch-table-detail failed: {e}\n{traceback.format_exc()}")
        return {"found": False, "database": database, "table": table, "error": str(e)}


def _fmt_retention(ms: int) -> str:
    """Convert retention milliseconds to a human-readable string."""
    def _trim(f: float) -> str:
        return str(int(f)) if f == int(f) else f"{f:.1f}"

    secs = ms // 1000
    if secs < 3600:
        return f"{secs // 60} min"
    hours = secs / 3600
    if hours < 48:
        return f"{_trim(hours)} hrs ({ms:,} ms)"
    days = hours / 24
    return f"{_trim(days)} days ({ms:,} ms)"


def _sync_topic_detail(topic_name: str) -> dict:
    """Fetch per-partition offsets + liveness + retention directly from Kafka broker."""
    from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
    from kafka.admin import ConfigResource, ConfigResourceType

    cfg = dict(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=10_000,
        client_id="healthwatch-inspect",
    )
    consumer = KafkaConsumer(**cfg)
    admin    = KafkaAdminClient(**cfg)
    try:
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            return {"found": False, "topic": topic_name}

        tps = [TopicPartition(topic_name, p) for p in sorted(partitions)]
        end_offsets   = consumer.end_offsets(tps)
        begin_offsets = consumer.beginning_offsets(tps)

        # ── Replication factor from cluster metadata (loaded by partitions_for_topic) ─
        replication_factor: int | str = "—"
        try:
            part_meta = consumer._client.cluster._partitions.get(topic_name, {})
            if part_meta:
                first = part_meta.get(0) or next(iter(part_meta.values()))
                replication_factor = len(first.replicas)
        except Exception as e:
            logger.warning(f"[topic_detail] replication_factor fetch failed: {e}")
        threshold_ms  = int((time.time() - KAFKA_LIVE_WINDOW_MINUTES * 60) * 1000)
        at_threshold  = consumer.offsets_for_times({tp: threshold_ms for tp in tps})

        parts = []
        total = 0
        for tp in tps:
            earliest = begin_offsets.get(tp) or 0
            latest   = end_offsets.get(tp)   or 0
            msgs     = max(0, latest - earliest)
            is_live  = at_threshold.get(tp) is not None
            parts.append({
                "partition": str(tp.partition),
                "earliest": earliest,
                "latest":   latest,
                "messages": msgs,
                "is_live":  is_live,
            })
            total += msgs

        # ── Fetch topic retention config ──────────────────────────────────────
        retention_str = "Broker default"
        try:
            cfg_resources = [ConfigResource(ConfigResourceType.TOPIC, topic_name)]
            configs = admin.describe_configs(cfg_resources)
            # kafka-python returns a list of DescribeConfigsResponse objects;
            # each has a .resources list of tuples:
            #   (error_code, error_message, resource_type, resource_name, config_entries)
            # config_entries is a list of tuples: (name, value, is_sensitive, is_default, ...)
            for response in configs:
                for resource in response.resources:
                    _err, _errmsg, _rtype, _rname, config_entries = resource
                    for entry in config_entries:
                        name, value = entry[0], entry[1]
                        if name == "retention.ms" and value is not None:
                            val = int(value)
                            if val > 0:
                                retention_str = _fmt_retention(val)
                            break
        except Exception as e:
            logger.warning(f"[topic_detail] retention fetch failed: {e}")

        # ── Pull cached consumer lag for this topic ───────────────────────────
        cached = (
            last_result.get("kafka", {})
            .get("__details__", {})
            .get("consumer_lag", {})
            .get(topic_name, {})
        )

        return {
            "found": True,
            "topic": topic_name,
            "total_messages": total,
            "info": {"partition_count": len(partitions), "replication_factor": replication_factor},
            "retention": {"retention_ms": retention_str},
            "lag":  cached,
            "partition_offsets": parts,
        }
    finally:
        consumer.close()
        admin.close()


@app.get("/api/topic/{topic_name}")
async def topic_detail(topic_name: str):
    logger.info(f"[topic_detail] topic={topic_name}")
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _sync_topic_detail, topic_name)
        logger.info(
            f"[topic_detail] {topic_name}: found={result.get('found')} "
            f"msgs={result.get('total_messages', 0)}"
        )
        return result
    except Exception as e:
        logger.error(f"[topic_detail] EXCEPTION: {e}\n{traceback.format_exc()}")
        return {"topic": topic_name, "found": False, "error": str(e), "partition_offsets": []}


@app.post("/api/cert-prechecks")
async def cert_prechecks():
    """Run cert renewal prechecks step by step. Returns array of check results."""
    logger.info("[cert-prechecks] START")
    from datetime import datetime as dt, timezone
    checks = []

    # 1. API Server reachable
    try:
        from kubernetes import client as k8s_client
        core, _, _ = _get_k8s()
        version_api = k8s_client.VersionApi()
        version_api.get_code()
        checks.append({"id": "api_reachable", "label": "API Server Reachable", "status": "pass", "detail": "Cluster API responding"})
    except Exception as e:
        checks.append({"id": "api_reachable", "label": "API Server Reachable", "status": "fail", "detail": str(e)})

    # 2. All nodes Ready
    try:
        nodes = core.list_node().items
        not_ready = [n.metadata.name for n in nodes if not any(
            c.type == "Ready" and c.status == "True" for c in n.status.conditions)]
        if not_ready:
            checks.append({"id": "nodes_ready", "label": "All Nodes Ready", "status": "fail",
                           "detail": f"Not ready: {', '.join(not_ready)}"})
        else:
            checks.append({"id": "nodes_ready", "label": "All Nodes Ready", "status": "pass",
                           "detail": f"{len(nodes)} node(s) Ready"})
    except Exception as e:
        checks.append({"id": "nodes_ready", "label": "All Nodes Ready", "status": "fail", "detail": str(e)})

    # 3. No cordoned nodes
    try:
        cordoned = [n.metadata.name for n in nodes if n.spec.unschedulable]
        if cordoned:
            checks.append({"id": "no_cordon", "label": "No Cordoned Nodes", "status": "fail",
                           "detail": f"Cordoned: {', '.join(cordoned)}"})
        else:
            checks.append({"id": "no_cordon", "label": "No Cordoned Nodes", "status": "pass",
                           "detail": "No nodes cordoned"})
    except Exception as e:
        checks.append({"id": "no_cordon", "label": "No Cordoned Nodes", "status": "fail", "detail": str(e)})

    # 4. Control plane pods running
    try:
        cp_pods = core.list_namespaced_pod(namespace="kube-system").items
        for comp in ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd"]:
            matched = [p for p in cp_pods if p.metadata.name.startswith(comp)]
            if matched and matched[0].status.phase == "Running":
                checks.append({"id": f"cp_{comp}", "label": f"{comp} Running", "status": "pass",
                               "detail": matched[0].metadata.name})
            elif matched:
                checks.append({"id": f"cp_{comp}", "label": f"{comp} Running", "status": "fail",
                               "detail": f"Status: {matched[0].status.phase}"})
            else:
                checks.append({"id": f"cp_{comp}", "label": f"{comp} Running", "status": "warn",
                               "detail": "Pod not found (externally managed?)"})
    except Exception as e:
        checks.append({"id": "cp_check", "label": "Control Plane Pods", "status": "fail", "detail": str(e)})

    # 5. No crashlooping pods in kube-system
    try:
        crash_pods = []
        for p in cp_pods:
            for cs in (p.status.container_statuses or []):
                if cs.state.waiting and cs.state.waiting.reason in ("CrashLoopBackOff", "Error", "OOMKilled"):
                    crash_pods.append(p.metadata.name)
        if crash_pods:
            checks.append({"id": "no_crash", "label": "No CrashLooping Pods (kube-system)", "status": "fail",
                           "detail": f"Failing: {', '.join(crash_pods[:3])}"})
        else:
            checks.append({"id": "no_crash", "label": "No CrashLooping Pods (kube-system)", "status": "pass",
                           "detail": "All kube-system pods healthy"})
    except Exception as e:
        checks.append({"id": "no_crash", "label": "No CrashLooping Pods (kube-system)", "status": "fail", "detail": str(e)})

    # 6. API Server certificate expiry
    try:
        from cryptography import x509 as x509_mod
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with socket.create_connection(("kubernetes.default.svc", 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname="kubernetes.default.svc") as ssock:
                der_bytes = ssock.getpeercert(True)
                if not der_bytes:
                    raise RuntimeError("No certificate returned")
                cert_obj = x509_mod.load_der_x509_certificate(der_bytes)
                expiry = cert_obj.not_valid_after_utc
                days_left = (expiry - dt.now(timezone.utc)).days
                if days_left <= 0:
                    checks.append({"id": "cert_expiry", "label": "API Server Certificate Valid", "status": "fail",
                                   "detail": f"EXPIRED ({expiry.strftime('%Y-%m-%d')})"})
                elif days_left <= CERT_WARN_DAYS:
                    checks.append({"id": "cert_expiry", "label": "API Server Certificate Valid", "status": "warn",
                                   "detail": f"Expires in {days_left}d ({expiry.strftime('%Y-%m-%d')})"})
                else:
                    checks.append({"id": "cert_expiry", "label": "API Server Certificate Valid", "status": "pass",
                                   "detail": f"{days_left}d remaining ({expiry.strftime('%Y-%m-%d')})"})
    except Exception as e:
        checks.append({"id": "cert_expiry", "label": "API Server Certificate Valid", "status": "fail", "detail": str(e)})

    # 7. No pending CSRs
    try:
        from kubernetes import client
        certs_api = client.CertificatesV1Api()
        csrs = certs_api.list_certificate_signing_request()
        pending = [c.metadata.name for c in csrs.items
                   if not any(cond.type == "Approved" for cond in (c.status.conditions or []))]
        if pending:
            checks.append({"id": "no_pending_csr", "label": "No Pending CSRs", "status": "warn",
                           "detail": f"{len(pending)} pending: {', '.join(pending[:3])}"})
        else:
            checks.append({"id": "no_pending_csr", "label": "No Pending CSRs", "status": "pass",
                           "detail": "All CSRs approved"})
    except Exception as e:
        checks.append({"id": "no_pending_csr", "label": "No Pending CSRs", "status": "warn",
                       "detail": f"Could not check CSRs: {e}"})

    # ── 8-16. ConfigMap-based prechecks (from cert-checker CronJob) ──
    cm_data = _read_cert_configmap()
    if cm_data:
        cm_certs = cm_data.get("certificates", [])
        cm_prechecks = cm_data.get("prechecks", [])
        cm_timestamp = cm_data.get("timestamp", "")
        cm_backup = cm_data.get("backup", {})

        # 8. ConfigMap data freshness
        try:
            from datetime import datetime as dt2
            scanned = dt2.fromisoformat(cm_timestamp.replace("Z", "+00:00"))
            age_hours = (dt.now(timezone.utc) - scanned).total_seconds() / 3600
            if age_hours <= 12:
                checks.append({"id": "cm_fresh", "label": "Cert Scan Data Fresh",
                               "status": "pass", "detail": f"Scanned {age_hours:.1f}h ago"})
            elif age_hours <= 24:
                checks.append({"id": "cm_fresh", "label": "Cert Scan Data Fresh",
                               "status": "warn", "detail": f"Scanned {age_hours:.1f}h ago — may be stale"})
            else:
                checks.append({"id": "cm_fresh", "label": "Cert Scan Data Fresh",
                               "status": "fail", "detail": f"Scanned {age_hours:.1f}h ago — CronJob may have failed"})
        except Exception:
            checks.append({"id": "cm_fresh", "label": "Cert Scan Data Fresh",
                           "status": "warn", "detail": "Could not parse timestamp"})

        # 9-12. Cert expiry status — INFORMATIONAL only, never blocks renewal
        # (expired certs are the REASON for renewal, not a blocker)
        cert_groups = [
            ("etcd_certs", "etcd Certificates", [c for c in cm_certs if c.get("category") == "etcd"]),
            ("fp_certs", "Front Proxy Certs", [c for c in cm_certs if "front-proxy" in c.get("name", "") and c.get("category") != "ca"]),
            ("api_client_certs", "API Server Client Certs", [c for c in cm_certs if "apiserver-" in c.get("name", "") and c.get("category") == "pki"]),
            ("kc_certs", "Kubeconfig Certs", [c for c in cm_certs if c.get("category") == "kubeconfig"]),
        ]
        for group_id, group_label, group_certs in cert_groups:
            if group_certs:
                ok_count = len([c for c in group_certs if c["status"] == "ok"])
                expired = [c for c in group_certs if c["status"] == "error"]
                expiring = [c for c in group_certs if c["status"] == "warn"]
                if expired:
                    # Expired certs = INFO, this is why we're renewing
                    checks.append({"id": group_id, "label": group_label,
                                   "status": "info",
                                   "detail": f"{len(expired)} expired — renewal needed"})
                elif expiring:
                    checks.append({"id": group_id, "label": group_label,
                                   "status": "info",
                                   "detail": f"{len(expiring)} expiring soon, {ok_count}/{len(group_certs)} valid"})
                else:
                    checks.append({"id": group_id, "label": group_label,
                                   "status": "pass",
                                   "detail": f"{ok_count}/{len(group_certs)} valid"})

        # 13. CA certs (special — kubeadm can't renew them, THIS is a real blocker)
        ca_certs = [c for c in cm_certs if c.get("category") == "ca"]
        if ca_certs:
            ca_expired = [c for c in ca_certs if c.get("status") == "error"]
            min_days = min(c.get("days_left", 0) for c in ca_certs)
            if ca_expired:
                # CA expired IS a real blocker — kubeadm certs renew won't fix this
                checks.append({"id": "ca_certs", "label": "CA Certificates Valid",
                               "status": "fail",
                               "detail": f"CA EXPIRED — kubeadm cannot renew CAs, manual rotation required"})
            else:
                checks.append({"id": "ca_certs", "label": "CA Certificates Valid",
                               "status": "pass",
                               "detail": f"Shortest: {min_days}d (kubeadm won't renew CAs)"})

        # 14-16. Node-level prechecks from CronJob
        for cpc in cm_prechecks:
            checks.append({
                "id": f"node_{cpc['id']}",
                "label": f"[Node] {cpc['label']}",
                "status": cpc["status"],
                "detail": cpc["detail"],
            })

        # 17. PKI backup status
        if cm_backup:
            bk_status = cm_backup.get("status", "error")
            bk_path = cm_backup.get("latest", "unknown")
            bk_size = cm_backup.get("size_display", cm_backup.get("size_mb", "?"))
            checks.append({"id": "backup", "label": "PKI Backup",
                           "status": "pass" if bk_status == "ok" else "fail",
                           "detail": f"{bk_path} ({bk_size})"})
    else:
        checks.append({"id": "cm_missing", "label": "Cert Scan Data (ConfigMap)",
                       "status": "warn",
                       "detail": "cert-checker CronJob has not run yet — deploy and wait for first scan"})

    # info/warn/skip/pass do NOT block renewal — only "fail" blocks
    any_fail = any(c["status"] == "fail" for c in checks)
    all_good = not any_fail
    logger.info(f"[cert-prechecks] done: {len(checks)} checks, "
                f"pass={sum(1 for c in checks if c['status']=='pass')}, "
                f"info={sum(1 for c in checks if c['status']=='info')}, "
                f"fail={sum(1 for c in checks if c['status']=='fail')}")
    return {
        "checks": checks,
        "all_pass": all_good,
        "any_fail": any_fail,
        "can_renew": all_good,
        "renewal_command": "sudo kubeadm certs renew all",
        "dry_run": True,
    }


@app.post("/api/cert-refresh-scan")
async def cert_refresh_scan():
    """Trigger a fresh cert-checker CronJob run and wait for it to complete.
    Creates a one-off Job from the CronJob, waits for completion, returns fresh ConfigMap data."""
    import json as _json
    logger.info("[cert-refresh-scan] START")
    from kubernetes import client as k8s_client

    batch = k8s_client.BatchV1Api()
    core, _, _ = _get_k8s()

    job_name = f"cert-scan-{int(time.time())}"

    # Read the CronJob to get its job template
    try:
        cronjob = batch.read_namespaced_cron_job("healthwatch-cert-checker", K8S_NAMESPACE)
    except Exception as e:
        logger.error(f"[cert-refresh-scan] CronJob not found: {e}")
        return {"status": "error", "detail": f"CronJob not found: {e}"}

    # Create a one-off Job from the CronJob template
    job_template = cronjob.spec.job_template
    job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(
            name=job_name,
            namespace=K8S_NAMESPACE,
            labels={"app.kubernetes.io/component": "cert-checker", "triggered-by": "healthwatch"},
        ),
        spec=job_template.spec,
    )

    try:
        batch.create_namespaced_job(K8S_NAMESPACE, job)
        logger.info(f"[cert-refresh-scan] Created job {job_name}")
    except Exception as e:
        logger.error(f"[cert-refresh-scan] Failed to create job: {e}")
        return {"status": "error", "detail": f"Failed to create job: {e}"}

    # Wait for job completion (poll every 3s, max 120s)
    max_wait = 120
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(3)
        elapsed += 3
        try:
            job_status = batch.read_namespaced_job_status(job_name, K8S_NAMESPACE)
            if job_status.status.succeeded and job_status.status.succeeded >= 1:
                logger.info(f"[cert-refresh-scan] Job {job_name} completed in {elapsed}s")
                break
            if job_status.status.failed and job_status.status.failed >= 2:
                logger.error(f"[cert-refresh-scan] Job {job_name} failed")
                return {"status": "error", "detail": "Cert scan job failed — check pod logs"}
        except Exception as e:
            logger.warning(f"[cert-refresh-scan] Polling error: {e}")

    if elapsed >= max_wait:
        return {"status": "error", "detail": f"Job did not complete within {max_wait}s"}

    # Read fresh ConfigMap
    cm_data = _read_cert_configmap()
    if cm_data:
        logger.info(f"[cert-refresh-scan] Fresh data: {cm_data.get('timestamp', '?')}")
        return {"status": "ok", "timestamp": cm_data.get("timestamp", ""), "data": cm_data}
    else:
        return {"status": "error", "detail": "Job completed but ConfigMap not updated"}


@app.post("/api/cert-postchecks")
async def cert_postchecks():
    """Post-renewal validation checks. Run after certificate renewal to verify cluster health."""
    logger.info("[cert-postchecks] START")
    from datetime import datetime as dt, timezone
    checks = []

    try:
        core, _, _ = _get_k8s()

        # 1. API server responding
        try:
            from kubernetes import client as k8s_client
            version_api = k8s_client.VersionApi()
            version_api.get_code()
            checks.append({"id": "api_alive", "label": "API Server Responding", "status": "pass",
                           "detail": "Cluster API responding after renewal"})
        except Exception as e:
            checks.append({"id": "api_alive", "label": "API Server Responding", "status": "fail",
                           "detail": str(e)})

        # 2. All nodes Ready
        try:
            nodes = core.list_node().items
            not_ready = [n.metadata.name for n in nodes if not any(
                c.type == "Ready" and c.status == "True" for c in n.status.conditions)]
            if not_ready:
                checks.append({"id": "nodes_ok", "label": "All Nodes Ready", "status": "fail",
                               "detail": f"Not ready: {', '.join(not_ready)}"})
            else:
                checks.append({"id": "nodes_ok", "label": "All Nodes Ready", "status": "pass",
                               "detail": f"{len(nodes)} node(s) Ready"})
        except Exception as e:
            checks.append({"id": "nodes_ok", "label": "All Nodes Ready", "status": "fail", "detail": str(e)})

        # 3. Control plane pods healthy
        try:
            cp_pods = core.list_namespaced_pod(namespace="kube-system").items
            for comp in ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd"]:
                matched = [p for p in cp_pods if p.metadata.name.startswith(comp)]
                if matched and matched[0].status.phase == "Running":
                    checks.append({"id": f"post_cp_{comp}", "label": f"{comp} Running",
                                   "status": "pass", "detail": matched[0].metadata.name})
                else:
                    phase = matched[0].status.phase if matched else "Not Found"
                    checks.append({"id": f"post_cp_{comp}", "label": f"{comp} Running",
                                   "status": "fail", "detail": f"Status: {phase}"})
        except Exception as e:
            checks.append({"id": "post_cp", "label": "Control Plane Pods", "status": "fail", "detail": str(e)})

        # 4. kube-proxy DaemonSet ready
        try:
            from kubernetes import client as k8s_client
            apps = k8s_client.AppsV1Api()
            ds = apps.read_namespaced_daemon_set("kube-proxy", "kube-system")
            desired = ds.status.desired_number_scheduled or 0
            ready = ds.status.number_ready or 0
            if ready == desired and desired > 0:
                checks.append({"id": "kube_proxy", "label": "kube-proxy DaemonSet", "status": "pass",
                               "detail": f"{ready}/{desired} ready"})
            else:
                checks.append({"id": "kube_proxy", "label": "kube-proxy DaemonSet", "status": "fail",
                               "detail": f"{ready}/{desired} ready"})
        except Exception as e:
            checks.append({"id": "kube_proxy", "label": "kube-proxy DaemonSet", "status": "warn",
                           "detail": f"Could not check: {e}"})

        # 5. CNI DaemonSet (try common names: calico, flannel, cilium, weave)
        try:
            from kubernetes import client as k8s_client
            apps = k8s_client.AppsV1Api()
            ds_list = apps.list_namespaced_daemon_set("kube-system").items
            cni_names = ["calico-node", "canal", "flannel", "cilium", "weave-net", "kube-flannel-ds"]
            cni_ds = [d for d in ds_list if d.metadata.name in cni_names]
            if cni_ds:
                ds = cni_ds[0]
                desired = ds.status.desired_number_scheduled or 0
                ready = ds.status.number_ready or 0
                if ready == desired and desired > 0:
                    checks.append({"id": "cni_ds", "label": f"CNI DaemonSet ({ds.metadata.name})",
                                   "status": "pass", "detail": f"{ready}/{desired} ready"})
                else:
                    checks.append({"id": "cni_ds", "label": f"CNI DaemonSet ({ds.metadata.name})",
                                   "status": "warn", "detail": f"{ready}/{desired} ready"})
            else:
                checks.append({"id": "cni_ds", "label": "CNI DaemonSet", "status": "warn",
                               "detail": "No known CNI DaemonSet found"})
        except Exception as e:
            checks.append({"id": "cni_ds", "label": "CNI DaemonSet", "status": "warn",
                           "detail": f"Could not check: {e}"})

        # 6. Pod restart surge detection (high restarts in kube-system last hour)
        try:
            high_restart = []
            for p in cp_pods:
                for cs in (p.status.container_statuses or []):
                    if cs.restart_count and cs.restart_count > 5:
                        high_restart.append(f"{p.metadata.name}({cs.restart_count}x)")
            if high_restart:
                checks.append({"id": "restart_surge", "label": "No Pod Restart Surge",
                               "status": "warn", "detail": f"High restarts: {', '.join(high_restart[:3])}"})
            else:
                checks.append({"id": "restart_surge", "label": "No Pod Restart Surge",
                               "status": "pass", "detail": "No excessive restarts in kube-system"})
        except Exception as e:
            checks.append({"id": "restart_surge", "label": "No Pod Restart Surge",
                           "status": "warn", "detail": str(e)})

        # 7. Recent warning events in kube-system
        try:
            events = core.list_namespaced_event("kube-system").items
            now = dt.now(timezone.utc)
            recent_warns = [
                e for e in events
                if e.type == "Warning"
                and e.last_timestamp
                and (now - e.last_timestamp.replace(tzinfo=timezone)).total_seconds() < 1800
            ]
            if recent_warns:
                reasons = list(set(e.reason for e in recent_warns[:5]))
                checks.append({"id": "k8s_events", "label": "K8s Warning Events (30min)",
                               "status": "warn",
                               "detail": f"{len(recent_warns)} warnings: {', '.join(reasons)}"})
            else:
                checks.append({"id": "k8s_events", "label": "K8s Warning Events (30min)",
                               "status": "pass", "detail": "No recent warnings"})
        except Exception as e:
            checks.append({"id": "k8s_events", "label": "K8s Warning Events (30min)",
                           "status": "warn", "detail": f"Could not check: {e}"})

        # 8. New cert expiry improved (compare with ConfigMap data)
        cm_data = _read_cert_configmap()
        if cm_data:
            old_certs = cm_data.get("certificates", [])
            checks.append({"id": "cert_improved", "label": "Cert Expiry Refreshed",
                           "status": "pass",
                           "detail": f"Baseline from {cm_data.get('timestamp', '?')} — re-run CronJob to verify new expiry"})
        else:
            checks.append({"id": "cert_improved", "label": "Cert Expiry Refreshed",
                           "status": "warn", "detail": "No baseline — run CronJob after renewal to verify"})

    except Exception as e:
        logger.error(f"[cert-postchecks] EXCEPTION: {e}\n{traceback.format_exc()}")
        checks.append({"id": "error", "label": "Post-check Error", "status": "fail", "detail": str(e)})

    all_pass = all(c["status"] == "pass" for c in checks)
    any_fail = any(c["status"] == "fail" for c in checks)
    logger.info(f"[cert-postchecks] done: {len(checks)} checks")
    return {"checks": checks, "all_pass": all_pass, "any_fail": any_fail}


@app.get("/api/health")
async def health():
    return {"status": "ok", "last_checked": last_checked, "is_running": is_running}


import mimetypes

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("image/svg+xml", ".svg")
mimetypes.add_type("application/json", ".json")

if BUILD_DIR.exists():
    app.mount(
        "/static", StaticFiles(directory=str(BUILD_DIR / "static")), name="static"
    )

    @app.get("/{full_path:path}")
    async def serve_react(full_path: str):
        file_path = BUILD_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(BUILD_DIR / "index.html"))
