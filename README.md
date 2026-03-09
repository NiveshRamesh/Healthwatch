# HealthWatch — React Migration Guide

## New Project Structure

```
D:\Vumonitor\files\
├── app/
│   └── main.py                ← FastAPI backend (edit per BACKEND_CHANGES.py)
├── frontend-src/              ← React source (NEW — rename from healthwatch-react)
│   ├── public/
│   │   └── index.html
│   ├── src/
│   │   ├── index.js
│   │   ├── index.css
│   │   ├── App.jsx
│   │   ├── utils.js
│   │   ├── hooks/
│   │   │   └── useHealthWatch.js
│   │   └── components/
│   │       ├── CheckRow.jsx
│   │       ├── ConsumerLagPanel.jsx
│   │       ├── LiveDataPanel.jsx
│   │       ├── Modal.jsx
│   │       ├── ServiceSection.jsx
│   │       ├── TopicDiagBar.jsx
│   │       └── Tooltip.jsx
│   └── package.json
├── requirements.txt
└── Dockerfile                 ← Updated (multi-stage build)
```

## Files to REMOVE from old project

| File/Folder       | Why                                      |
|-------------------|------------------------------------------|
| `templates/`      | Entire folder — replaced by React        |
| `templates/index.html` | Replaced by React build             |

## Files to ADD

| File/Folder       | What                                     |
|-------------------|------------------------------------------|
| `frontend-src/`   | Entire React source from this package    |
| `Dockerfile`      | Replace old Dockerfile with new one      |

## Changes to app/main.py

Open `app/main.py` and make these edits:

### 1. Replace old imports (top of file)
```python
# REMOVE:
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# ADD:
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
```

### 2. Add CORS middleware (right after `app = FastAPI(...)`)
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 3. Remove Jinja setup
```python
# REMOVE these two lines:
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
```

### 4. Add static file serving + catch-all (at the bottom, after all /api routes)
```python
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")

@app.get("/{full_path:path}")
async def serve_react(full_path: str):
    return FileResponse("frontend/index.html")
```

### 5. Update requirements.txt — add this line
```
python-multipart
```

---

## Running Locally from VS Code

### Prerequisites (install once)
- Node.js 20+: https://nodejs.org
- Python 3.11+: already have it
- VS Code with Python extension

### Step 1 — Start the FastAPI backend

Open a terminal in VS Code (`Ctrl+`` `):
```bash
cd D:\Vumonitor\files

# Install Python deps if needed
pip install -r requirements.txt

# Run FastAPI
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```
Backend runs at: http://localhost:8080

### Step 2 — Start the React frontend

Open a SECOND terminal in VS Code:
```bash
cd D:\Vumonitor\files\frontend-src

# Install Node deps (first time only)
npm install

# Start React dev server
npm start
```
React runs at: http://localhost:3000
It auto-proxies all `/healthwatch/api/*` calls to port 8080 (via `"proxy"` in package.json).

### Step 3 — Open in browser
Go to: **http://localhost:3000**

Hot reload works — edit any `.jsx` file and browser updates instantly. No rebuild needed.

---

## Deploying (Docker — same as before)

```cmd
cd D:\Vumonitor\files

docker build -t healthwatch:latest .
docker save healthwatch:latest -o healthwatch.tar
scp healthwatch.tar ubuntu@<server>:~/
ssh ubuntu@<server>
sudo ctr -n k8s.io images import ~/healthwatch.tar
helm upgrade healthwatch . -n vsmaps
kubectl rollout restart deployment/healthwatch -n vsmaps
```

The Dockerfile now does a multi-stage build:
1. Stage 1: `npm run build` → produces optimized React files
2. Stage 2: Copies build output into Python image at `./frontend/`
3. FastAPI serves `frontend/index.html` for all non-API routes

---

## API routes (no changes needed in backend)

| Method | Route                          | Used by              |
|--------|--------------------------------|----------------------|
| GET    | `/healthwatch/api/status`      | Auto-refresh, polling|
| POST   | `/healthwatch/api/run`         | Run button           |
| GET    | `/healthwatch/api/topic/{name}`| Topic inspect        |
| GET    | `/healthwatch/api/kafka/topics`| (available)          |
