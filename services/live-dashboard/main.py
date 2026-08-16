"""
Live Dashboard
==============

A deliberately lightweight FastAPI service that serves a single static page
(`static/index.html`) rendering the WebSocket live feed exposed by the
query-api service (`/ws/live`). No build step, no Node toolchain, no bundler
— just the same Python/FastAPI stack used everywhere else in this platform,
serving vanilla JS + Chart.js (via CDN) to the browser, so the whole
platform stays on a single backend stack end to end.
"""

import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

QUERY_API_WS_URL = os.getenv("QUERY_API_WS_URL", "ws://localhost:8000/ws/live")
QUERY_API_HTTP_URL = os.getenv("QUERY_API_HTTP_URL", "http://localhost:8000")

app = FastAPI(title="Signal Intelligence Live Dashboard", version="1.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "live-dashboard"}


@app.get("/config.js")
async def config_js():
    """Serves runtime configuration as a small JS snippet, so the same built
    static assets work unmodified across docker-compose, Helm, and local
    dev — only the environment variables change."""
    js = (
        f"window.SIGNAL_DASHBOARD_CONFIG = {{\n"
        f'  wsUrl: "{QUERY_API_WS_URL}",\n'
        f'  apiUrl: "{QUERY_API_HTTP_URL}"\n'
        f"}};\n"
    )
    return HTMLResponse(content=js, media_type="application/javascript")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8003)
