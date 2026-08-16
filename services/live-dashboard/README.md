# Live Dashboard

A single-page, no-build-step dashboard that visualizes the query-api
WebSocket live feed (`/ws/live`) using vanilla JS and Chart.js (loaded from
a CDN). Served by a minimal FastAPI app — same stack as the rest of the
platform, no Node toolchain required.

## Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export QUERY_API_WS_URL=ws://localhost:8000/ws/live
export QUERY_API_HTTP_URL=http://localhost:8000
uvicorn main:app --reload --port 8003
```

Open `http://localhost:8003`.
