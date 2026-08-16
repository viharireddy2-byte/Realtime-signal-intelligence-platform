"""Lightweight API-key authentication.

Disabled by default (``API_KEY_REQUIRED=false``) so local development and the
docker-compose stack work out of the box. Set ``API_KEY_REQUIRED=true`` and
``API_KEY=<secret>`` (or inject via Kubernetes Secret, see
``infra/helm/signal-intel-platform/values.yaml``) to require callers to send
an ``X-API-Key`` header for every request except health/readiness probes.

This intentionally stays simple (a shared-secret header) rather than
reimplementing OAuth2/JWT — see docs/SECURITY.md for how to layer real
identity-aware auth (e.g. an API gateway or JWT-issuing auth service) in
front of this in a production deployment.
"""

from fastapi import Header, HTTPException, status

from config import settings


async def require_api_key(x_api_key: str = Header(default=None)) -> None:
    if not settings.api_key_required:
        return
    if not x_api_key or x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
        )
