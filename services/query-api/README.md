# Query API

FastAPI service exposing the platform's read surface: hot KPIs (Redis),
historical series and sessions (TimescaleDB), anomaly alerts, and a
WebSocket live feed.

## Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export REDIS_HOST=localhost POSTGRES_HOST=localhost
uvicorn main:app --reload
```

Interactive docs: `http://localhost:8000/docs`

See [`../../docs/API.md`](../../docs/API.md) for the full endpoint reference
and [`../../docs/DATABASE.md`](../../docs/DATABASE.md) for the schema.
