# Security

This document describes what's actually implemented today, what's configuration-away-from-implemented, and what would need real work before this platform touched production traffic or real user data.

## What's implemented today

### API authentication

`services/query-api/security.py` implements a shared-secret `X-API-Key` header check, off by default (`API_KEY_REQUIRED=false`) so local development and the docker-compose stack work without extra setup. Enable it with `API_KEY_REQUIRED=true` and a real `API_KEY` value for anything beyond local dev — see [`DEPLOYMENT.md`](DEPLOYMENT.md). Health/readiness probes intentionally stay open so load balancers and Kubernetes don't need credentials.

This is a shared secret, not identity-aware auth — it tells you a request came from *someone with the key*, not *which* client or user. For multi-tenant or user-facing deployments, put a real OAuth2/JWT-issuing auth service or API gateway in front of it and keep the API key (or mTLS) for service-to-service calls behind that gateway.

### Rate limiting

`slowapi` enforces `RATE_LIMIT` (default `600/minute` per client IP) on every data endpoint in query-api.

### Data anonymization

`ingestion/event-producer/event_producer.py` hashes `user_id` with SHA-256 (truncated to 16 hex chars) before the event is ever serialized to Kafka — raw user identifiers never enter the pipeline. See `EventGenerator._hash_user_id`.

### Non-root containers

Every Dockerfile in this repo creates and switches to a non-root `appuser` (UID 1000) before running the application.

### Secrets via environment / Kubernetes Secrets

No service hardcodes credentials. Docker Compose passes them as plain environment variables (fine for local dev — see below for why that's not fine in production); the Helm chart's `templates/secret.yaml` creates a Kubernetes `Secret` and mounts values from it via `secretKeyRef`.

## What's configuration-away-from-implemented

### Notification channels

Email (SMTP), Slack, and custom webhooks are all real, working code paths in `services/notifier-service/main.py` — they're simply disabled by default (`EMAIL_ENABLED=false`, etc.) so the service doesn't try to reach external infrastructure it hasn't been told about. Flip the flags and supply credentials to activate them; see `docs/DEPLOYMENT.md`.

### TLS

Docker Compose runs everything in plaintext on an isolated bridge network (`signal-intel-network`), which is standard for local development. For any real deployment:

- Terminate TLS at an ingress/load balancer in front of query-api and live-dashboard
- Enable Kafka SASL/SSL (`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, keystore/truststore config) rather than the `PLAINTEXT` listener docker-compose uses
- Require `sslmode=require` (or `verify-full`) on the TimescaleDB connection strings, and set `redis.tls.enabled` if running Redis outside a trusted private network

## What's not implemented (and would need real work)

Being upfront about this matters more than pretending otherwise:

- **Kafka has no authentication** in `infra/docker-compose/docker-compose.yml` (`PLAINTEXT` listeners). Fine on an isolated Docker network for local dev; add SASL/SCRAM + ACLs before running Kafka anywhere it's reachable outside a trusted network.
- **The Helm chart's default `values.yaml` ships placeholder secrets** (`password`, `change-me-in-production`). These exist so `helm install` works out of the box for a demo cluster — they are not safe defaults for anything real. Override every value under `secrets:` and `postgresql.auth` from a real secret manager (Vault, cloud Secrets Manager, Sealed Secrets, External Secrets Operator) before any non-local install.
- **No mutual TLS between services.** Every internal call (query-api → Redis/TimescaleDB, notifier-service → Kafka, etc.) trusts the network it's running on. A production deployment on a shared cluster should add a service mesh (Istio/Linkerd) or explicit mTLS.
- **No audit logging.** Requests are logged (via each service's `logging.basicConfig`), but there's no structured, tamper-evident audit trail of who accessed what. Add one before handling data subject to compliance requirements (SOC 2, GDPR, etc.).
- **No automated vulnerability scanning of running images**, only the CI-time Trivy filesystem scan. Add image scanning (Trivy/Snyk/Grype) to the image-build step and a runtime scanner (Falco or similar) for a hardened deployment.

## Reporting a vulnerability

If you find a security issue in this project, please open a private security advisory on GitHub (Security tab → "Report a vulnerability") rather than a public issue.
