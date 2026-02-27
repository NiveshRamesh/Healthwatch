from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
import asyncio
import httpx
import asyncpg
import aiokafka
from aiokafka.admin import AIOKafkaAdminClient
import clickhouse_connect
from kubernetes import client as k8s_client, config as k8s_config
from datetime import datetime
import json
import os
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config from env vars ───────────────────────────────────────────────────
CLICKHOUSE_HOST     = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ZOOKEEPER_HOST      = os.getenv("ZOOKEEPER_HOST", "zookeeper")
ZOOKEEPER_PORT      = int(os.getenv("ZOOKEEPER_PORT", "2181"))

POSTGRES_DSN        = os.getenv("POSTGRES_DSN", "postgresql://user:password@postgres:5432/appdb")

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY    = os.getenv("MINIO_SECRET_KEY", "minioadmin")

K8S_NAMESPACE       = os.getenv("K8S_NAMESPACE", "default")
MONITORED_PODS      = os.getenv("MONITORED_PODS", "denver,nairobi,cairo,kafka,zookeeper,clickhouse,postgres,minio").split(",")

# ─── State store ─────────────────────────────────────────────────────────────
last_result: dict = {}
last_checked: str = ""
is_running: bool = False

# ─── Health check functions ──────────────────────────────────────────────────

async def check_clickhouse() -> dict:
    checks = {}
    try:
        c = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            connect_timeout=5
        )
        result = c.query("SELECT 1")
        checks["Connection"] = {"status": "ok", "detail": "Reachable"}
        try:
            tables = c.query("SELECT count() FROM system.tables")
            checks["System Tables"] = {"status": "ok", "detail": f"{tables.result_rows[0][0]} tables"}
        except Exception as e:
            checks["System Tables"] = {"status": "warn", "detail": str(e)}
        try:
            c.query("SELECT 1")
            checks["Query Execution"] = {"status": "ok", "detail": "Queries executing normally"}
        except Exception as e:
            checks["Query Execution"] = {"status": "error", "detail": str(e)}
        c.close()
    except Exception as e:
        checks["Connection"] = {"status": "error", "detail": str(e)}
        checks["System Tables"] = {"status": "unknown", "detail": "Skipped"}
        checks["Query Execution"] = {"status": "unknown", "detail": "Skipped"}
    return checks

async def check_kafka() -> dict:
    checks = {}
    try:
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
        await admin.start()
        topics = await admin.list_topics()
        checks["Broker Connection"] = {"status": "ok", "detail": f"Connected to {KAFKA_BOOTSTRAP}"}
        checks["Topic Listing"] = {"status": "ok", "detail": f"{len(topics)} topics found"}
        await admin.close()
    except Exception as e:
        checks["Broker Connection"] = {"status": "error", "detail": str(e)}
        checks["Topic Listing"] = {"status": "unknown", "detail": "Skipped"}

    # Zookeeper check via TCP
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ZOOKEEPER_HOST, ZOOKEEPER_PORT), timeout=5
        )
        writer.write(b"ruok")
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4), timeout=3)
        writer.close()
        if data == b"imok":
            checks["Zookeeper"] = {"status": "ok", "detail": "Responded imok"}
        else:
            checks["Zookeeper"] = {"status": "warn", "detail": f"Unexpected response: {data}"}
    except Exception as e:
        checks["Zookeeper"] = {"status": "error", "detail": str(e)}
    return checks

async def check_postgres() -> dict:
    checks = {}
    try:
        conn = await asyncio.wait_for(asyncpg.connect(POSTGRES_DSN), timeout=5)
        checks["Connection"] = {"status": "ok", "detail": "Connected successfully"}
        try:
            version = await conn.fetchval("SELECT version()")
            checks["Version"] = {"status": "ok", "detail": version.split(",")[0]}
        except Exception as e:
            checks["Version"] = {"status": "warn", "detail": str(e)}
        try:
            await conn.fetchval("SELECT 1")
            checks["Query Execution"] = {"status": "ok", "detail": "Queries running normally"}
        except Exception as e:
            checks["Query Execution"] = {"status": "error", "detail": str(e)}
        await conn.close()
    except Exception as e:
        checks["Connection"] = {"status": "error", "detail": str(e)}
        checks["Version"] = {"status": "unknown", "detail": "Skipped"}
        checks["Query Execution"] = {"status": "unknown", "detail": "Skipped"}
    return checks

async def check_minio() -> dict:
    checks = {}
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            r = await http.get(f"{MINIO_ENDPOINT}/minio/health/live")
            if r.status_code == 200:
                checks["Liveness"] = {"status": "ok", "detail": "MinIO is live"}
            else:
                checks["Liveness"] = {"status": "warn", "detail": f"Status {r.status_code}"}
            r2 = await http.get(f"{MINIO_ENDPOINT}/minio/health/ready")
            if r2.status_code == 200:
                checks["Readiness"] = {"status": "ok", "detail": "MinIO is ready"}
            else:
                checks["Readiness"] = {"status": "warn", "detail": f"Status {r2.status_code}"}
    except Exception as e:
        checks["Liveness"] = {"status": "error", "detail": str(e)}
        checks["Readiness"] = {"status": "unknown", "detail": "Skipped"}
    return checks

async def check_kubernetes() -> dict:
    checks = {}
    try:
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()

        v1 = k8s_client.CoreV1Api()
        pods = v1.list_namespaced_pod(namespace=K8S_NAMESPACE)
        pod_map = {p.metadata.name: p for p in pods.items}

        for pod_prefix in MONITORED_PODS:
            pod_prefix = pod_prefix.strip()
            matching = [p for name, p in pod_map.items() if pod_prefix.lower() in name.lower()]
            if not matching:
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "error", "detail": "No pod found"}
                continue
            pod = matching[0]
            phase = pod.status.phase
            name = pod.metadata.name
            if phase == "Running":
                # Check all containers ready
                container_statuses = pod.status.container_statuses or []
                all_ready = all(c.ready for c in container_statuses)
                restarts = sum(c.restart_count for c in container_statuses)
                if all_ready and restarts < 5:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "ok", "detail": f"{name} — Running, {restarts} restarts"}
                elif restarts >= 5:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn", "detail": f"{name} — {restarts} restarts (high)"}
                else:
                    checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn", "detail": f"{name} — Not all containers ready"}
            elif phase == "Pending":
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "warn", "detail": f"{name} — Pending"}
            else:
                checks[f"{pod_prefix.capitalize()} Pod"] = {"status": "error", "detail": f"{name} — {phase}"}

        # Node check
        nodes = v1.list_node()
        ready_nodes = 0
        for node in nodes.items:
            for cond in node.status.conditions:
                if cond.type == "Ready" and cond.status == "True":
                    ready_nodes += 1
        checks["Cluster Nodes"] = {"status": "ok" if ready_nodes > 0 else "error",
                                    "detail": f"{ready_nodes}/{len(nodes.items)} nodes Ready"}

    except Exception as e:
        checks["Kubernetes API"] = {"status": "error", "detail": str(e)}
    return checks

# ─── Aggregate runner ─────────────────────────────────────────────────────────

async def run_all_checks():
    global last_result, last_checked, is_running
    if is_running:
        return
    is_running = True
    logger.info("Starting health checks...")
    try:
        results = await asyncio.gather(
            check_clickhouse(),
            check_kafka(),
            check_postgres(),
            check_minio(),
            check_kubernetes(),
            return_exceptions=True
        )
        labels = ["clickhouse", "kafka", "postgres", "minio", "kubernetes"]
        last_result = {}
        for label, res in zip(labels, results):
            if isinstance(res, Exception):
                last_result[label] = {"Error": {"status": "error", "detail": str(res)}}
            else:
                last_result[label] = res
        last_checked = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Health checks completed at {last_checked}")
    finally:
        is_running = False

# ─── App lifecycle ────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Run once on startup
    asyncio.create_task(run_all_checks())
    # Schedule twice daily: 8am and 8pm
    scheduler.add_job(run_all_checks, "cron", hour="8,20", minute=0)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="HealthWatch", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# ─── API Routes ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/status")
async def get_status():
    return {
        "last_checked": last_checked,
        "is_running": is_running,
        "results": last_result
    }

@app.post("/api/run")
async def trigger_checks(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_checks)
    return {"message": "Health checks triggered"}
