"""Centralized configuration for the notifier-service."""

import os


class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "signalintel")
    postgres_user: str = os.getenv("POSTGRES_USER", "signalintel_admin")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "password")

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    alerts_topic: str = os.getenv("ALERTS_TOPIC", "signal.alerts.v1")
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP", "notifier-service")

    # Notification channels — all optional and disabled unless configured
    email_enabled: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    email_recipients: list = [
        addr.strip() for addr in os.getenv("EMAIL_RECIPIENTS", "").split(",") if addr.strip()
    ]
    smtp_host: str = os.getenv("SMTP_HOST", "")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_user: str = os.getenv("SMTP_USER", "")
    smtp_password: str = os.getenv("SMTP_PASSWORD", "")
    smtp_from: str = os.getenv("SMTP_FROM", "alerts@signal-intel.local")

    slack_enabled: bool = os.getenv("SLACK_ENABLED", "false").lower() == "true"
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")

    webhook_enabled: bool = os.getenv("WEBHOOK_ENABLED", "false").lower() == "true"
    custom_webhooks: list = [
        url.strip() for url in os.getenv("CUSTOM_WEBHOOKS", "").split(",") if url.strip()
    ]


settings = Settings()
