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
import asyncio, os, logging, traceback, time
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
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
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
            },
            "pods_pvcs": {"__pods_pvcs__": safe(pvc_pods, "Pods/PVCs")},
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
app = FastAPI(title="HealthWatch Phase 2", root_path="/healthwatch")
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
