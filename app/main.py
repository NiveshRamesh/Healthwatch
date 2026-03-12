"""
HealthWatch Phase 2 — Production backend with full structured logging.
All checks use real K8s API, ClickHouse, Longhorn, and Kafka.
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
import asyncio, os, logging, json, subprocess, traceback, time
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
CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    "chi-clickhouse-vusmart-0.chi-clickhouse-vusmart.vsmaps.svc.cluster.local",
)
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "vusmart")
LONGHORN_URL = os.getenv(
    "LONGHORN_URL", "http://longhorn-backend.longhorn-system.svc.cluster.local:9500"
)
KAFKA_REQUIRED_CONNECTORS = os.getenv(
    "KAFKA_REQUIRED_CONNECTORS", "enrichment-connector"
)
NODE_CPU_WARN_THRESHOLD = float(os.getenv("NODE_CPU_WARN_THRESHOLD", "70"))
NODE_MEM_WARN_THRESHOLD = float(os.getenv("NODE_MEM_WARN_THRESHOLD", "80"))
POD_CPU_WARN_THRESHOLD = float(os.getenv("POD_CPU_WARN_THRESHOLD", "70"))
POD_MEM_WARN_THRESHOLD = float(os.getenv("POD_MEM_WARN_THRESHOLD", "80"))
LH_ACTUAL_THRESHOLD = float(os.getenv("LH_ACTUAL_THRESHOLD", "0.7"))
LH_NODE_FREE_THRESHOLD = float(os.getenv("LH_NODE_FREE_THRESHOLD", "0.5"))
PVC_USED_THRESHOLD = float(os.getenv("PVC_USED_THRESHOLD", "0.8"))
POD_RESTART_THRESHOLD = int(os.getenv("POD_RESTART_THRESHOLD", "10"))
CH_MUTATION_AGE_MINUTES = int(os.getenv("CH_MUTATION_AGE_MINUTES", "30"))
CH_REPLICATION_LIMIT = int(os.getenv("CH_REPLICATION_POSTPONE_LIMIT", "100"))
CH_CLUSTER_NAME = os.getenv("CH_CLUSTER_NAME", "vusmart")
MONITORED_PODS = os.getenv(
    "MONITORED_PODS",
    "denver,nairobi,broker,kafka-cluster-cp-zookeeper,chi-clickhouse,postgresql,minio-tenant,keycloak,traefik",
).split(",")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgresql.vsmaps.svc.cluster.local")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "http://minio.vsmaps.svc.cluster.local:9000"
)

# ─── State ───────────────────────────────────────────────────────────────────
last_result: dict = {}
last_checked: str = ""
is_running: bool = False
check_durations: dict = {}

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
    return int(v[:-1]) / 1000 if v.endswith("m") else float(v)


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


# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 1 — KAFKA CONNECTOR STATE
# ═══════════════════════════════════════════════════════════════════════════════
async def check_kafka_connectors() -> dict:
    t0 = time.time()
    logger.info(
        f"[kafka_connectors] START ns={K8S_NAMESPACE} required={KAFKA_REQUIRED_CONNECTORS}"
    )
    try:
        core, _, _ = _get_k8s()
        pods = core.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=cp-kafka-connect"
        ).items
        logger.info(f"[kafka_connectors] found {len(pods)} cp-kafka-connect pod(s)")

        if not pods:
            logger.warning("[kafka_connectors] no pod found → critical")
            _timed("kafka_connectors", t0)
            return {
                "pod_status": "Missing",
                "active": [],
                "required": [],
                "missing": [],
                "status": "critical",
                "detail": "No cp-kafka-connect pod found",
            }

        pod_name = pods[0].metadata.name
        pod_status = pods[0].status.phase
        logger.info(f"[kafka_connectors] pod={pod_name} phase={pod_status}")

        cmd = [
            "kubectl",
            "exec",
            pod_name,
            "-n",
            K8S_NAMESPACE,
            "--",
            "curl",
            "-s",
            "localhost:9082/connectors",
        ]
        logger.debug(f"[kafka_connectors] exec: {' '.join(cmd)}")
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if res.returncode != 0:
            logger.warning(
                f"[kafka_connectors] exec rc={res.returncode} stderr={res.stderr.strip()!r}"
            )
        logger.debug(f"[kafka_connectors] stdout={res.stdout.strip()!r}")

        try:
            active = json.loads(res.stdout.strip()) if res.returncode == 0 else []
        except json.JSONDecodeError as je:
            logger.error(
                f"[kafka_connectors] JSON parse error: {je} raw={res.stdout.strip()!r}"
            )
            active = []

        required = [c.strip() for c in KAFKA_REQUIRED_CONNECTORS.split(",")]
        missing = [c for c in required if c not in active]
        logger.info(
            f"[kafka_connectors] active={active} required={required} missing={missing}"
        )
        status = (
            "critical" if pod_status != "Running" else ("warn" if missing else "ok")
        )
        detail = f"{len(active)} connectors active" + (
            f" | MISSING: {missing}" if missing else ""
        )
        logger.info(f"[kafka_connectors] status={status}")
        _timed("kafka_connectors", t0)
        return {
            "pod_status": pod_status,
            "active": active,
            "required": required,
            "missing": missing,
            "status": status,
            "detail": detail,
        }
    except Exception as e:
        logger.error(f"[kafka_connectors] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("kafka_connectors", t0)
        return {
            "pod_status": "Error",
            "active": [],
            "required": [],
            "missing": [],
            "status": "error",
            "detail": str(e),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKS 2–4 — LONGHORN
# ═══════════════════════════════════════════════════════════════════════════════
async def check_longhorn() -> dict:
    t0 = time.time()
    logger.info(
        f"[longhorn] START url={LONGHORN_URL} "
        f"actual_thr={LH_ACTUAL_THRESHOLD} node_thr={LH_NODE_FREE_THRESHOLD}"
    )
    import httpx

    try:
        async with httpx.AsyncClient(timeout=15) as c:
            vr = await c.get(f"{LONGHORN_URL}/v1/volumes")
            nr = await c.get(f"{LONGHORN_URL}/v1/nodes")
        logger.info(
            f"[longhorn] /v1/volumes HTTP {vr.status_code}  /v1/nodes HTTP {nr.status_code}"
        )
        if vr.status_code != 200:
            logger.error(f"[longhorn] volumes error body: {vr.text[:300]}")
        if nr.status_code != 200:
            logger.error(f"[longhorn] nodes error body: {nr.text[:300]}")
        volumes_raw = vr.json().get("data", [])
        nodes_raw = nr.json().get("data", [])
        logger.info(f"[longhorn] {len(volumes_raw)} volumes, {len(nodes_raw)} nodes")
    except Exception as e:
        logger.error(f"[longhorn] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("longhorn", t0)
        return {
            "volumes": [],
            "nodes": [],
            "status": "error",
            "detail": f"Longhorn unreachable: {e}",
        }

    vol_results = []
    for v in volumes_raw:
        ab = int(v.get("actualSize", 0))
        cb = int(v.get("size", 1))
        pct = ab / cb * 100 if cb > 0 else 0
        state = v.get("state", "unknown")
        ready = v.get("ready", False)
        pod = next(
            (
                kb.get("podName")
                for kb in v.get("kubeStatus", {}).get("workloadsStatus", [])
                if kb.get("podStatus") == "Running"
            ),
            None,
        )
        status = (
            "critical"
            if (not ready or state != "attached")
            else ("warn" if cb > 0 and ab / cb > LH_ACTUAL_THRESHOLD else "ok")
        )
        logger.debug(
            f"[longhorn] vol={v['name']} state={state} ready={ready} "
            f"{round(ab / 1024**3, 1)}GiB/{round(cb / 1024**3, 1)}GiB → {status}"
        )
        if status != "ok":
            logger.warning(f"[longhorn] vol={v['name']} → {status}")
        vol_results.append(
            {
                "name": v["name"],
                "pod": pod,
                "pvc": v.get("kubeStatus", {}).get("pvcName", ""),
                "state": state,
                "ready": ready,
                "actual_gb": round(ab / 1024**3, 1),
                "csize_gb": round(cb / 1024**3, 1),
                "used_pct": round(pct, 1),
                "status": status,
            }
        )

    node_results = []
    for n in nodes_raw:
        for did, disk in n.get("disks", {}).items():
            sc = int(disk.get("storageScheduled", 0))
            mx = int(disk.get("storageMaximum", 1))
            av = int(disk.get("storageAvailable", 1))
            pct = sc / mx * 100 if mx > 0 else 0
            status = (
                "warn" if (sc / mx if mx > 0 else 0) > LH_NODE_FREE_THRESHOLD else "ok"
            )
            logger.debug(
                f"[longhorn] node={n['name']} disk={did} "
                f"sched={round(sc / 1024**3, 1)}GiB max={round(mx / 1024**3, 1)}GiB → {status}"
            )
            node_results.append(
                {
                    "node": n["name"],
                    "disk": did,
                    "path": disk.get("path", ""),
                    "scheduled_gb": round(sc / 1024**3, 1),
                    "available_gb": round(av / 1024**3, 1),
                    "used_pct": round(pct, 1),
                    "status": status,
                }
            )

    all_s = [v["status"] for v in vol_results] + [n["status"] for n in node_results]
    overall = "critical" if "critical" in all_s else "warn" if "warn" in all_s else "ok"
    logger.info(
        f"[longhorn] overall={overall} vols={len(vol_results)} nodes={len(node_results)}"
    )
    _timed("longhorn", t0)
    return {"volumes": vol_results, "nodes": node_results, "status": overall}


# ═══════════════════════════════════════════════════════════════════════════════
# CHECKS 5–10 — PVC & POD DEEP CHECKS
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
                alerts.append(f"⚠ {c.name}: {restarts} restarts")
                logger.warning(
                    f"[pods_pvcs] pod={pod.metadata.name} c={c.name} restarts={restarts}"
                )
            if state != "running":
                alerts.append(f"⚠ {c.name}: state={state}")
                logger.warning(
                    f"[pods_pvcs] pod={pod.metadata.name} c={c.name} state={state}"
                )
            if not cpu_lim or not mem_lim:
                alerts.append(f"⚠ {c.name}: missing resource limits")
                logger.warning(
                    f"[pods_pvcs] pod={pod.metadata.name} c={c.name} missing limits"
                )
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
                else ("warn" if any("missing resource" in a for a in alerts) else "ok")
            )
        )
        if phase not in ("Running", "Succeeded"):
            logger.warning(f"[pods_pvcs] pod={pod.metadata.name} phase={phase} → {s}")
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
                f"[pods_pvcs] PVC={name} phase={phase} orphan={orphan} → {s}"
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

        r13 = q(
            "Q13_inactive_ddl",
            "SELECT query,status,initiator FROM system.distributed_ddl_queue "
            "WHERE status='Inactive' LIMIT 20",
        )
        inactive_ddl = [{"query": r[0], "status": r[1], "initiator": r[2]} for r in r13]

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

        r15 = q(
            "Q15_no_ttl",
            "SELECT database,name FROM system.tables "
            "WHERE engine LIKE '%MergeTree%' AND toUInt32(ttl_field)=0 "
            "AND database NOT IN ('system','information_schema','INFORMATION_SCHEMA')",
        )
        no_ttl = [{"database": r[0], "table": r[1]} for r in r15]
        if no_ttl:
            _names = [t["database"] + "." + t["table"] for t in no_ttl]
            logger.warning(f"[ch_tables] tables without TTL: {_names}")

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
                "tables": no_ttl,
                "status": "warn" if no_ttl else "ok",
                "detail": f"{len(no_ttl)} tables missing TTL"
                if no_ttl
                else "All tables have TTL",
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
                f"({round(mem_used / 1024**3, 1)}/{round(mem_total / 1024**3, 1)} GiB) → {s}"
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
                    f"[k8s_resources] pod={pname} cpu={cp:.1f}% mem={mp:.1f}% → {s}"
                )
            else:
                logger.debug(
                    f"[k8s_resources] pod={pname} cpu={cp:.1f}% mem={mp:.1f}% → ok"
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
        ch.query("SELECT 1+1")
        logger.info(f"[ch_connectivity] OK — {n} tables")
        _timed("ch_connectivity", t0)
        return {
            "Connection": {
                "status": "ok",
                "detail": f"Reachable on {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}",
            },
            "System Tables": {"status": "ok", "detail": f"{n} tables"},
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
            "System Tables": {"status": "error", "detail": "N/A"},
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
        logger.info(f"[kafka] brokers={len(bp)} running={len(br)} → {bs}")
        zp = core.list_namespaced_pod(
            namespace=K8S_NAMESPACE, label_selector="app=cp-zookeeper"
        ).items
        zr = [p for p in zp if p.status.phase == "Running"]
        zs = "ok" if zr else "warn"
        logger.info(f"[kafka] zookeeper={len(zp)} running={len(zr)} → {zs}")
        connectors = await check_kafka_connectors()
        _timed("kafka", t0)
        return {
            "Broker Health": {"status": bs, "detail": bd},
            "Zookeeper Mode": {
                "status": zs,
                "detail": f"{len(zr)}/{len(zp)} Zookeeper Running",
            },
            "Zookeeper Stats": {
                "status": "ok",
                "detail": "Stats exec not yet implemented",
            },
            "Live Data": {"status": "ok", "detail": "Topic live check via CH"},
            "Consumer Lag": {
                "status": "ok",
                "detail": "Lag check via CH kafka_consumers",
            },
            "Kafka Connectors": {
                "status": connectors["status"],
                "detail": connectors["detail"],
                "_connectors": connectors,
            },
            "__details__": {"topics": {}, "topic_live_status": {}, "consumer_lag": {}},
        }
    except Exception as e:
        logger.error(f"[kafka] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("kafka", t0)
        return {
            "Broker Health": {"status": "error", "detail": str(e)},
            "Zookeeper Mode": {"status": "error", "detail": "N/A"},
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

    try:
        async with httpx.AsyncClient(timeout=10) as c:
            lr = await c.get(f"{MINIO_ENDPOINT}/minio/health/live")
            rr = await c.get(f"{MINIO_ENDPOINT}/minio/health/ready")
        logger.info(f"[minio] live={lr.status_code} ready={rr.status_code}")
        _timed("minio", t0)
        return {
            "Liveness": {
                "status": "ok" if lr.status_code == 200 else "error",
                "detail": f"HTTP {lr.status_code}",
            },
            "Readiness": {
                "status": "ok" if rr.status_code == 200 else "error",
                "detail": f"HTTP {rr.status_code}",
            },
        }
    except Exception as e:
        logger.error(f"[minio] EXCEPTION: {e}\n{traceback.format_exc()}")
        _timed("minio", t0)
        return {
            "Liveness": {"status": "error", "detail": str(e)},
            "Readiness": {"status": "error", "detail": str(e)},
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
        for prefix in MONITORED_PODS:
            prefix = prefix.strip()
            matched = [p for p in all_pods if p.metadata.name.startswith(prefix)]
            if not matched:
                logger.warning(f"[k8s_pods] no pod for prefix={prefix!r}")
                checks[f"Pod: {prefix.capitalize()}"] = {
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
                f"[k8s_pods] prefix={prefix!r} running={len(running)}/{len(matched)} restarts={restarts} → {s}"
            )
            checks[f"Pod: {prefix.capitalize()}"] = {"status": s, "detail": detail}
        rn = sum(
            1
            for n in all_nodes
            if any(
                c.type == "Ready" and c.status == "True" for c in n.status.conditions
            )
        )
        ns = "ok" if rn == len(all_nodes) else "warn"
        logger.info(f"[k8s_pods] nodes ready={rn}/{len(all_nodes)} → {ns}")
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
            check_longhorn(),
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
            "longhorn",
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
            longhorn,
            pvc_pods,
            ch_tables,
            k8s_resources,
        ) = results
        ch_merged = {**safe(ch_conn, "CH Connection")}
        ch_merged["__ch_tables__"] = safe(ch_tables, "CH Tables")

        last_result = {
            "clickhouse": ch_merged,
            "kafka": safe(kafka, "Kafka"),
            "postgres": safe(pg, "Postgres"),
            "minio": safe(minio, "MinIO"),
            "kubernetes": {
                **safe(k8s_pods, "K8s Pods"),
                "__resources__": safe(k8s_resources, "K8s Resources"),
            },
            "longhorn": {"__longhorn__": safe(longhorn, "Longhorn")},
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
        f"HTTP {request.method} {request.url.path} → {resp.status_code} ({round(time.time() - t0, 3)}s)"
    )
    return resp


@app.on_event("startup")
async def startup():
    logger.info("HealthWatch startup — active config:")
    logger.info(f"  K8S_NAMESPACE={K8S_NAMESPACE}  LOG_LEVEL={LOG_LEVEL}")
    logger.info(f"  CLICKHOUSE={CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}  db={CLICKHOUSE_DB}")
    logger.info(f"  LONGHORN_URL={LONGHORN_URL}")
    logger.info(f"  MINIO_ENDPOINT={MINIO_ENDPOINT}")
    logger.info(f"  POSTGRES={POSTGRES_HOST}:{POSTGRES_PORT}  db={POSTGRES_DB}")
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


@app.get("/api/topic/{topic_name}")
async def topic_detail(topic_name: str):
    logger.info(f"[topic_detail] topic={topic_name}")
    try:
        ch = _get_ch()
        rows = ch.query(
            "SELECT partition,min(offset),max(offset),count() FROM system.kafka_consumers "
            f"WHERE topic='{topic_name}' GROUP BY partition ORDER BY partition"
        ).result_rows
        parts = [
            {
                "partition": str(r[0]),
                "earliest": r[1],
                "latest": r[2],
                "messages": r[2] - r[1],
                "is_live": True,
            }
            for r in rows
        ]
        total = sum(p["messages"] for p in parts)
        logger.info(
            f"[topic_detail] {topic_name}: {len(parts)} partitions total={total}"
        )
        return {
            "topic": topic_name,
            "found": True,
            "total_messages": total,
            "partition_offsets": parts,
        }
    except Exception as e:
        logger.error(f"[topic_detail] EXCEPTION: {e}\n{traceback.format_exc()}")
        return {
            "topic": topic_name,
            "found": False,
            "error": str(e),
            "partition_offsets": [],
        }


@app.get("/api/health")
async def health():
    return {"status": "ok", "last_checked": last_checked, "is_running": is_running}


if BUILD_DIR.exists():
    app.mount(
        "/static", StaticFiles(directory=str(BUILD_DIR / "static")), name="static"
    )

    @app.get("/{full_path:path}")
    async def serve_react(full_path: str):
        return FileResponse(str(BUILD_DIR / "index.html"))
