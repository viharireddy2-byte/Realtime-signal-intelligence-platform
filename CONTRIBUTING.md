# Contributing

## Development workflow

1. Fork and clone the repository.
2. Run `./scripts/setup-local-dev.sh` to get the full stack running locally.
3. Make your change. Match the existing style of the file/service you're touching — see below for per-language conventions.
4. Add or update tests. New behavior in `services/query-api` or `services/notifier-service` should get a `tests/unit` test at minimum; anything touching Redis/TimescaleDB reads/writes should get a `tests/integration` test. New Flink/Spark logic should get a JUnit test under the job's `src/test/java`.
5. Run the relevant test suite locally before opening a PR (see the root `README.md` "Testing" section).
6. Open a pull request against `main`. CI (`.github/workflows/ci-cd.yml`) will lint, test, build images, and scan automatically.

## Code style

- **Python**: `black` for formatting, `flake8` for linting (both run in CI). Type hints are encouraged but not required everywhere — `main.py`/`config.py` in each service are good examples of the expected style.
- **Java**: Standard Java conventions, 4-space indent. Keep Flink/Spark job logic testable independently of the cluster runtime where possible (see `EventAggregatorTest.java` / `RollingStatsTest.java` for the pattern — test the `AggregateFunction`/state class directly, not the whole DataStream pipeline).
- **JavaScript (k6, live-dashboard)**: no build step, no framework — keep it vanilla and dependency-light.

## Commit messages

Short, imperative summary line (`Add EWMA anomaly detector`, not `Added` or `Adding`), with a body explaining *why* when the change isn't self-evident from the diff.

## Adding a new streaming job

1. Create `streaming-jobs/<job-name>/` with its own `pom.xml` (copy an existing job's as a starting point — keep the `groupId` `com.signalintel.platform`).
2. Add a package under `com.signalintel.platform.<domain>`.
3. Read all configuration from environment variables with sensible defaults (see `System.getenv().getOrDefault(...)` in the existing jobs) — no hardcoded bootstrap servers or credentials.
4. Add the job to `streaming-jobs/README.md`'s table and `scripts/submit-jobs.sh`.
5. If it needs a new Kafka topic, add it to `scripts/setup-local-dev.sh`'s topic list and `docs/DATABASE.md`.

## Adding a new service

1. Create `services/<service-name>/` with `main.py`, `requirements.txt`, `Dockerfile`, and a `README.md`.
2. Follow the existing single-file-app-plus-small-modules pattern (`main.py` + `config.py` + any small supporting module) rather than introducing a new framework or folder convention.
3. Expose `/health` at minimum; add `/metrics` (prometheus-client) if the service does meaningful work.
4. Add it to `infra/docker-compose/docker-compose.yml`, the CI workflow's service matrices, and the Helm chart if it needs to run in Kubernetes.

## Reporting bugs / requesting features

Open a GitHub issue with as much detail as you can: what you expected, what happened, and steps to reproduce (for bugs), or the use case you're trying to solve (for features).
