# Notifier Service

Consumes `signal.alerts.v1`, applies rule + cooldown logic, persists to
TimescaleDB, and dispatches email/Slack/webhook notifications. Also accepts
Alertmanager webhooks so infrastructure alerts share the same pipeline.

## Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092 POSTGRES_HOST=localhost
uvicorn main:app --reload --port 8001
```

## Enabling notification channels

All channels are off by default. Enable via environment variables:

```bash
# Slack
export SLACK_ENABLED=true
export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...

# Email (SMTP)
export EMAIL_ENABLED=true
export SMTP_HOST=smtp.example.com
export SMTP_USER=alerts@example.com
export SMTP_PASSWORD=...
export EMAIL_RECIPIENTS=oncall@example.com,platform-team@example.com

# Arbitrary webhooks
export WEBHOOK_ENABLED=true
export CUSTOM_WEBHOOKS=https://example.com/hooks/signal-alerts
```

See [`../../docs/API.md`](../../docs/API.md) for endpoint details.
