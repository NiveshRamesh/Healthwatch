# HealthWatch Phase 2 — Local Run Guide

Complete implementation of all 22 checks from the vuHealth Go script.

## What's New vs Phase 1

| Section | New Checks |
|---|---|
| **Kafka** | #1 Connector state (cp-kafka-connect pod + enrichment-connector) |
| **Longhorn** | #2 Volume actual size, #3 Volume state/ready, #4 Node disk usage |
| **Pods/PVCs** | #5 PVC disk %, #6 Resource limits, #7 Restart count, #8 Container state, #9 PVC phase, #10 Orphan PVC |
| **ClickHouse** | #11 Unused Kafka tables, #12 Read-only tables, #13 Inactive DDL queries, #14 Long mutations, #15 No TTL, #16 Detached parts, #17 Table sizes, #18 Replication stuck, #19 Replica inconsistency |
| **Kubernetes** | #20 Node CPU+Memory, #21 Pod CPU+Memory |

---

## Prerequisites

```bash
# Python 3.11+
python --version

# Node 18+
node --version
```

---

## Step 1 — Backend (FastAPI)

```bash
# From the project root (healthwatch-phase2/)

# Create venv
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# Install deps
pip install fastapi uvicorn httpx

# Run
uvicorn app.main:app --reload --port 8081
```

Backend runs at: http://localhost:8081
API status:      http://localhost:8081/healthwatch/api/status

---

## Step 2 — Frontend (React)

```bash
# In a new terminal
cd frontend

npm install

npm start
```

Frontend runs at: http://localhost:3000

The `proxy` in package.json routes `/healthwatch/*` → `http://localhost:8081`
so no CORS issues in dev.

---

## Step 3 — Open the UI

Go to: **http://localhost:3000**

Click **▶ RUN DIAGNOSTICS** to see all checks populate.

The UI uses mock data by default — every check is fully visible and interactive.

---

## Project Structure

```
healthwatch-phase2/
├── app/
│   └── main.py              ← FastAPI backend (all 22 checks)
├── frontend/
│   ├── public/index.html
│   ├── package.json          ← proxy to :8081
│   └── src/
│       ├── App.jsx            ← main layout, section ordering
│       ├── index.css          ← design tokens (unchanged)
│       ├── utils.js           ← SECTIONS_META + new sections
│       ├── hooks/
│       │   └── useHealthWatch.js
│       └── components/
│           ├── Shared.jsx           ← Badge, ProgressBar, SubSection, Chip, Tip
│           ├── ServiceSection.jsx   ← routes svcKey → correct panel
│           ├── ClickHousePanel.jsx  ← 3 existing + 9 new CH checks
│           ├── KafkaPanel.jsx       ← existing + connector sub-panel
│           ├── KubernetesPanel.jsx  ← existing pods + node/pod resource panels
│           ├── LonghornPanel.jsx    ← NEW: volumes + node disk (checks 2-4)
│           ├── PodsPVCsPanel.jsx    ← NEW: pod health + PVC status (checks 5-10)
│           ├── CheckRow.jsx         ← unchanged
│           ├── TopicDiagBar.jsx     ← unchanged
│           ├── LiveDataPanel.jsx    ← unchanged
│           ├── ConsumerLagPanel.jsx ← unchanged
│           ├── Modal.jsx            ← unchanged
│           └── Tooltip.jsx          ← unchanged
└── requirements.txt
```

---

## Mock vs Real

All checks in `app/main.py` have a `# ── MOCK ──` section.

To connect to real infrastructure, replace the mock block with the real K8s/CH call.
Each function has a docstring describing exactly what the real implementation does.

### Example — replace mock with real ClickHouse:
```python
async def check_clickhouse_tables() -> dict:
    # Remove mock block, uncomment real code:
    import clickhouse_connect
    c = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, ...)
    count = c.query("SELECT count(*) FROM system.kafka_consumers ...").result_rows[0][0]
    ...
```

---

## Environment Variables

```bash
# Kafka
KAFKA_REQUIRED_CONNECTORS=enrichment-connector

# K8s node/pod resource thresholds
NODE_CPU_WARN_THRESHOLD=70
NODE_MEM_WARN_THRESHOLD=80
POD_CPU_WARN_THRESHOLD=70
POD_MEM_WARN_THRESHOLD=80

# Longhorn
LH_ACTUAL_THRESHOLD=0.7       # volume size alert at 70% of csize
LH_NODE_FREE_THRESHOLD=0.5    # node disk alert at 50% scheduled/available

# PVC/Pod
PVC_USED_THRESHOLD=0.8
POD_RESTART_THRESHOLD=10

# ClickHouse
CH_MUTATION_AGE_MINUTES=30
CH_REPLICATION_POSTPONE_LIMIT=100
CH_CLUSTER_NAME=vusmart
```

---

## Deploying to Server (after local testing)

1. Copy `app/main.py` → `D:\Vumonitor\files\app\main.py`
2. Copy all `frontend/src/` changes → `D:\Vumonitor\files\frontend\src\`
3. Update `helm-charts/healthwatch/templates/configmap.yaml` with new env vars
4. Update `helm-charts/healthwatch/templates/rbac.yaml` with new RBAC rules
5. Run Jenkins pipeline as normal
