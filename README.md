# HealthWatch — Infrastructure Self-Monitor Service

A standalone FastAPI service that runs health checks on all your infrastructure
components and displays them in a clean diagnostic dashboard — JioFiber-style.

## What it monitors

| Service       | Checks                                                     |
|---------------|------------------------------------------------------------|
| ClickHouse    | Connection, system tables reachable, query execution       |
| Kafka         | Broker connection, topic listing, Zookeeper ruok/imok      |
| PostgreSQL    | Connection, version, query execution                       |
| MinIO         | /minio/health/live, /minio/health/ready                    |
| Kubernetes    | Each named pod (phase, readiness, restart count), nodes    |

## Schedule
- **Automatic**: runs at **08:00** and **20:00** daily
- **Manual**: click "RUN DIAGNOSTICS" button in the UI any time

---

## Local Development

```bash
cd healthwatch
pip install -r requirements.txt

# Set env vars (or create a .env file)
export CLICKHOUSE_HOST=localhost
export KAFKA_BOOTSTRAP=localhost:9092
export POSTGRES_DSN=postgresql://user:pass@localhost:5432/appdb
export MINIO_ENDPOINT=http://localhost:9000
export K8S_NAMESPACE=default
export MONITORED_PODS=denver,nairobi,cairo,kafka,zookeeper,clickhouse,postgres,minio

uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
# Open http://localhost:8080
```

---

## Docker Build & Push

```bash
# Build
docker build -t your-registry/healthwatch:latest .

# Push
docker push your-registry/healthwatch:latest
```

---

## Kubernetes Deployment

### 1. Update the manifests

Edit `k8s/manifests.yaml` and replace:
- `namespace: default` → your actual namespace everywhere
- `your-registry/healthwatch:latest` → your actual image path
- Connection strings in ConfigMap and Secret

### 2. Update credentials in Secret

```bash
# Option A: edit stringData directly in manifests.yaml (auto-encoded)
# Option B: create from literals
kubectl create secret generic healthwatch-secrets \
  --from-literal=CLICKHOUSE_PASSWORD=yourpass \
  --from-literal=POSTGRES_DSN=postgresql://user:pass@postgres:5432/appdb \
  --from-literal=MINIO_ACCESS_KEY=minioadmin \
  --from-literal=MINIO_SECRET_KEY=minioadmin \
  -n your-namespace
```

### 3. Apply all manifests

```bash
kubectl apply -f k8s/manifests.yaml
```

### 4. Verify pod is running

```bash
kubectl get pods -n your-namespace | grep healthwatch
kubectl logs -f deployment/healthwatch -n your-namespace
```

### 5. Access the dashboard

```bash
kubectl port-forward svc/healthwatch 8080:8080 -n your-namespace
# Open http://localhost:8080
```

---

## Project Structure

```
healthwatch/
├── app/
│   └── main.py          # FastAPI app, all health check logic
├── templates/
│   └── index.html       # Dashboard UI
├── k8s/
│   └── manifests.yaml   # ServiceAccount, RBAC, ConfigMap, Secret, Deployment, Service
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Adding More Checks Later

To add a new service check, add a new async function in `app/main.py`:

```python
async def check_myservice() -> dict:
    checks = {}
    try:
        # your check logic
        checks["Connection"] = {"status": "ok", "detail": "Connected"}
    except Exception as e:
        checks["Connection"] = {"status": "error", "detail": str(e)}
    return checks
```

Then add it to `run_all_checks()` and add its metadata to `SECTIONS_META` in `index.html`.

---

## Notes

- The pod uses a **ClusterRole** with read-only access to pods and nodes.
- All sensitive values go in the **Secret**, not the ConfigMap.
- The service is **ClusterIP** only — no external exposure. Always access via port-forward.
- This is designed to be **completely separate** from your main application stack.
