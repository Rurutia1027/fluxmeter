# FluxMeter API Contract & Regression Test Plan

**Branch:** `test/contract-api-regression`  
**Stack:** pytest + httpx (no Playwright)  
**OpenAPI:** [`spec/openapi/openapi.yaml`](../../spec/openapi/openapi.yaml)  
**Profile map:**

- [`../profiles.yaml`](../profiles.yaml)
- runner: [`../../scripts/run-test-report.sh`](../../scripts/run-test-report.sh)
  **Deploy kinds:** `docker-lite` · `docker-full` · `k8s-lite` · `k8s-full` · `ci-lite` (default pipeline)

## Benefits

1. **CI automation** - Wire Docker Lite into the regression pipeline; on each Lite bring-up, run the profile and emit
   reports with no manual steps.
2. **Pre-release full gate** - Before a new version ships, run Full profiles (`docker-full` / `k8s-full`) for a complete
   stability report.
3. **Core-path safety net** - As the codebase (including AI-assisted changes) grows, keep ingest -> usage/budget and
   related contracts under automated cover.
4. **Deploy evidence** - Same suites corroborate k8s (and Docker) API availability and core-chain health after apply.

## Two pillars

| Pillar                              | Purpose                                                                           | Report                                |
|-------------------------------------|-----------------------------------------------------------------------------------|---------------------------------------|
| **1. OpenAPI contract**             | Shared HTTP contract tests; point at any deploy via `FLUXMETER_API`               | Contract coverage + core-path         |
| **2. Grouped suites + dual report** | Reuse existing cases in **named groups**; each deploy profile selects a group set | **(A)** logic · **(B)** deploy-health |

Reuse rules: **do not fork** tests per environment - tag/group once, select with `--profile`.


---

## Test groups (reuse inventory)

Groups are the unit of selection. Machine-readable list: `tests/profiles.yaml`.

| Group id     | What                              | Typical paths / make                                  | Live stack?                  |
|--------------|-----------------------------------|-------------------------------------------------------|------------------------------|
| `unit`       | Pure logic, no API                | `test_auth_unit`, pricing, billing export, gateway, … | No                           |
| `unit_redis` | Lua / rollup / buckets            | `test_lite_aggregate_unit`, `test_rollup`, …          | Redis                        |
| `java`       | Flink/Java unit                   | `./gradlew test`                                      | No                           |
| `contract`   | OpenAPI HTTP contract + core-path | `tests/contract/`                                     | API up                       |
| `lite_api`   | Lite production / overlay         | `test_lite_production`, `test_prod_overlay`           | Lite API (+ Redis)           |
| `e2e_full`   | Streaming correctness             | `test_integration`, `test_e2e_v2`                     | **Full** (Kafka + Flink job) |
| `saas`       | Control plane (optional)          | `test_control_plane`, …                               | SaaS compose                 |

Markers (pytest): `contract`, `lite_api`, `e2e`, `unit_redis` -- see `tests/pytest.ini`. Heavy e2e stays marked `e2e` (
already used).

---

## Deploy profile → group matrix

| Profile           | When to use                                | Groups included                                                       | Default CI?                                  | Deploy-health evidence                                                              |
|-------------------|--------------------------------------------|-----------------------------------------------------------------------|----------------------------------------------|-------------------------------------------------------------------------------------|
| **`ci-lite`**     | PR / main pipeline                         | `unit` + `java` + `contract` (+ `unit_redis` if Redis service in job) | **Yes**                                      | Contract core-path only (fast)                                                      |
| **`docker-lite`** | Local / compose Lite bring-up              | `contract` + `lite_api` (+ optional `unit_redis`)                     | No                                           | health → ingest → usage; lite overlay                                               |
| **`docker-full`** | Local / compose Full                       | `contract` + `e2e_full` (+ optional `lite_api` N/A)                   | **No** (too heavy / flaky for PR CI)         | Contract + **e2e** = Full chain proof                                               |
| **`k8s-lite`**    | Slim k8s Lite after apply                  | Same as `docker-lite` (URL = Ingress/port-forward)                    | Optional post-deploy job                     | Same as docker-lite against cluster URL                                             |
| **`k8s-full`**    | k8s Full (API + Kafka + Flink) after apply | `contract` + **`e2e_full`** (required)                                | **No** — post-deploy / nightly / manual gate | **Primary evidence** that the Full deploy is available and the streaming path works |
| **`logic`**       | Offline correctness only                   | `unit` + `java` (+ `unit_redis` if Redis)                             | Can run without compose                      | N/A (no deploy-health)                                                              |

### Why e2e is out of default CI but required on k8s-full

- **CI (`ci-lite`)** needs fast, stable signal: unit + Java + OpenAPI contract against a short-lived Lite compose. Full
  e2e needs Flink job submit, window waits, and heavier resources - poor default PR gate.
- **`k8s-full` / `docker-full`** need exactly that e2e suite as **deploy-health + availability** proof: not only
  `/health`, but ingest -> pipeline -> usage/budget behavior under the real Full topology. Treat `e2e_full` as *
  *required evidence** for Full deploy sign-off, not as optional fluff. 

```text
ci-lite:     unit | java | contract              → logic-report + light deploy-health
docker-lite: contract | lite_api                 → deploy-health (lite)
k8s-lite:    contract | lite_api                 → deploy-health (cluster URL)
docker-full: contract | e2e_full                 → deploy-health (full stack)
k8s-full:    contract | e2e_full                 → deploy-health (full cluster) 
```

---

## Pillar 1 - Contract suite (shared)

### Layout

```text
tests/contract/
  TEST_PLAN.md
  README.md
  conftest.py
  test_health_contract.py
  test_openapi_schema.py
  test_ingest_contract.py
  test_usage_contract.py
  test_budget_contract.py
  test_auth_contract.py
  test_core_path_health.py
  report_contract_coverage.py
```

### Cases (P0)

1. `GET /heath` - 200; `mode  ∈ {lite,full}`.
2. OpenAPI parses; P0 paths present.
3. Ingest 202 / 4xx shapes.
4. Usage smoke (Full may short-poll).
5. API Key auth as implemented today.

P1: batch limit, budget, optional schemathesis. Streaming replay details stay in `docs/runbooks/dlq-replay.md`.

```bash
FLUXMETER_API=http://127.0.0.1:8000 make test-contract
# k8s:
FLUXMETER_API=https://fluxmeter.example make test-contract 
```

---

## Pillar 2 - Runner, reports, wiring

### Runner

```bash
./scripts/run-test-report.sh --profile ci-lite
./scripts/run-test-report.sh --profile docker-lite
./scripts/run-test-report.sh --profile docker-full
./scripts/run-test-report.sh --profile k8s-lite
./scripts/run-test-report.sh --profile k8s-full   # contract + e2e_full → deploy-health
./scripts/run-test-report.sh --profile logic
```

Make:

```make
test-contract:                          # group: contract
test-report-logic:                      # profile: logic
test-report PROFILE=ci-lite|docker-lite|docker-full|k8s-lite|k8s-full
```

Bring-up of compose/k8s is **outside** the runner (or optional flags later). Runner assumes `FLUXMETER_API` reachable
when the profile includes `contract` / `lite_api` / `e2e_full`.

### Reports (`reports/`)

| Report               | File                              | Filled by                                                  |
|----------------------|-----------------------------------|------------------------------------------------------------|
| **A. Logic**         | `logic-report.json` (+ junit/xml) | Profiles that run `unit` / `java` / `unit_redis`           |
| **B. Deploy health** | `deploy-health.json` (+ short md) | Any profile with `contract` and/or `e2e_full` / `lite_api` |

`deploy-health.json` includes `profile`, `base_url`, `health`, `core_path`, `groups_run`, and for Full profiles an
`e2e_full: pass|fail` field — that field is the **availability evidence** for k8s-full / docker-full.

## Acceptance

- [ ] `tests/profiles.yaml` lists groups ↔ paths and profiles ↔ groups
- [ ] `scripts/run-test-report.sh --profile …` selects groups (no e2e in `ci-lite`)
- [ ] `k8s-full` / `docker-full` **require** `e2e_full` for deploy-health success
- [ ] `tests/contract/` P0 + markers; `make test-contract`
- [ ] Dual reports under `reports/`
- [ ] README documents profile matrix; link from [`../TEST_PLAN.md`](../TEST_PLAN.md)

## Implementation order

1. `profiles.yaml` + markers + runner stub (this branch).
2. Contract P0 package + coverage.
3. Wire existing paths into groups; emit dual reports.
4. CI job = `--profile ci-lite` only; document post-deploy `k8s-full` / `docker-full`.

---

## One-liner

**Same test groups, different deploy profiles** — CI stays Lite+contract; **Full (Docker or k8s) must run `e2e_full` as
deploy-health proof.**
