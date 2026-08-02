# Kubernetes verification

Single acceptance checklist for kind / K8s deploys. Assert **what you deployed** — skip rows for components not
installed yet. Not a load test.

**Plan:** [k8s-deployment-plan.md](k8s-deployment-plan.md) (P1–P4)
**Platform:** [deploy/platform/README.md](../deploy/platform/README.md)
**DNS:** [deploy/platform/SERVICE-DNS.md](../deploy/platform/SERVICE-DNS.md)

---

## Gates

| Gate                        | Pass means                                                                                                |
|-----------------------------|-----------------------------------------------------------------------------------------------------------|
| **Pods / Services**         | In-scope Deployments/StatefulSets `Ready`; Services resolve via `*.fluxmeter.svc.cluster.local`           |
| **Dashboard health green**  | Grafana (or Prometheus targets) show in-scope components **up** — no red/firing critical for idle cluster |
| **Component working state** | Matches topology you deployed (e.g. Kafka **3/3** brokers; Flink job `RUNNING` if submitted)              |
| **Small-N core path**       | Fixed tiny payload (N≤20) → **assertable** Redis keys / API JSON when app path is on                      |

---

## Checklist (one suite)

Run against the live cluster; mark N/A if that piece is not deployed.

| Area              | Expected                                                                                      | Phase hint |
|-------------------|-----------------------------------------------------------------------------------------------|------------|
| Redis             | `PING` → `PONG`; AOF / `noeviction` present; 3-node HA shows primary + replicas when deployed | P1         |
| Kafka             | 3 brokers Ready; topics listed; under-replicated = 0                                          | P1         |
| Flink Operator    | Operator Ready; REST reachable                                                                | P1         |
| ClickHouse (opt.) | `SELECT 1`; Kafka engine / MV against bootstrap DNS when enabled                              | P1         |
| Grafana / scrape  | In-scope rows **green** (Redis · Kafka · Flink · CH · API as deployed)                        | P2         |
| Tempo / traces    | Tempo Ready; Grafana Explore → Tempo finds `{resource.service.name="fluxmeter-flink"}` when Flink up | P2    |
| Secrets           | Passwords from Secret (not ConfigMap); apps/platform mount `secretKeyRef` / `envFrom`         | P3         |
| API               | `GET /health` → ok (Redis reachable, auth path if P3 on)                                      | P4         |
| Flink job (Full)  | Job `RUNNING`; after small produce, lag drains; Redis keys + API reflect N events             | P4         |
| Optional CH lag   | Same N: Redis/API fast; CH eventually consistent — **not** a billing fail                     | P4         |

**Regression id:** `k8s-smoke` (scripts/kubectl + dashboard + optional thin pytest/curl).

```text
1. Pods Ready + DNS resolves
2. Health green for in-scope boards
3. If API/Flink up: produce N token-events (fixed seed)
4. Wait ≤ watermark/window SLA (e.g. 2–3 min on kind)
5. Assert Redis / API delta == expected for that N
6. Optional: one budget deny; optional CH baseline query
```

ClickHouse is **out of band** for billing asserts unless the run explicitly sets `baseline=true`.

---

## Dashboard “green”

1. Scrape target `up` (or Redis datasource query succeeds).
2. No firing **critical** alert for Redis down / all brokers down / Flink job missing on a healthy idle or post-smoke
   cluster.
3. Panels show expected cardinality (e.g. Kafka brokers = 3 when 3 brokers deployed).

Mapping: [deploy/platform/observability/README.md](../deploy/platform/observability/README.md).

---

## Makefile / CI (aspirational)

| Suite id    | Intended entry        |
|-------------|-----------------------|
| `k8s-smoke` | `make test-k8s-smoke` |

Existing `make test-lite` / `make test-e2e` stay **compose-oriented**.

---

## Tracking

| Suite             | Status      |
|-------------------|-------------|
| k8s-smoke         | Spec only   |
| Make targets / CI | Not started |
