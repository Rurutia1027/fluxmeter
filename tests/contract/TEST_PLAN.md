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

