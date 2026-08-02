# Kubernetes Deployment Plan

**Source of truth** for getting Full-mode FluxMeter onto Kubernetes.

**Status:** Active · platform WIP in [`deploy/platform/`](../deploy/platform/)  
**Prod requirements:** [production-deploy.md](production-deploy.md) · **DNS:**
[deploy/platform/SERVICE-DNS.md](../deploy/platform/SERVICE-DNS.md) · **Verify:**
[k8s-verification.md](k8s-verification.md)

```text
docker-compose          <  local kind (this plan)  <  full prod K8s / managed
(make demo-full)           heavier than compose         MSK / ElastiCache / EKS
                           lighter than full prod
```

**Verification rule:** one smoke checklist — dashboard **health green** + in-scope components working + **small-N**
core path when apps are up. See [k8s-verification.md](k8s-verification.md).

---

## Gap vs production-deploy.md (honest)

| Component            | Prod target                                                   | In repo today                                                                                                                    |
|----------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| **Redis**            | 3 primary + 3 replica cluster (or managed), AOF, `noeviction` | **Partial** — **3×** StatefulSet (1 primary + 2 replicas), AOF + ClusterIP ([deploy/platform/redis/](../deploy/platform/redis/)) |
| **Kafka**            | ≥3 brokers, RF=3, `min.insync.replicas=2`                     | Kustomize **3** brokers + topics Job ([deploy/platform/kafka/](../deploy/platform/kafka/))                                       |
| **Flink**            | Operator + multi-TM, S3 checkpoints                           | Operator notes + kind `FlinkDeployment` skeleton                                                                                 |
| **ClickHouse**       | Optional baseline (Kafka engine + MV); **not** billing SoR    | Kustomize ready ([deploy/platform/clickhouse/](../deploy/platform/clickhouse/))                                                  |
| **Grafana / alerts** | Unified boards for Redis · Kafka · Flink · CH                 | Compose dashboard exists; kind observability still thin                                                                          |
| **Secrets**          | Passwords / tokens via K8s Secret (not plaintext ConfigMap)   | **Missing** — Redis/`requirepass`, CH, Grafana admin still ad-hoc                                                                |
| **App**              | API + Flink job wired to platform DNS, stable `/health` path  | Minimal API Helm chart (`deploy/helm/fluxmeter`); not fully integrated                                                           |

---

## Phase map

```text
P1  Kustomize platform     production-deploy shaped data plane via kubectl apply -k
P2  Unified monitoring     one Grafana/Prometheus board set for Redis · Kafka · Flink · CH
P3  Refine passwords      Secret-held creds; no ConfigMap/Git plaintext; NetworkPolicy/TLS notes
P4  App integration        API + Flink on platform DNS → stable usable metering service
```

Optional branch stacking: `deploy/k8s-p1-platform` → `deploy/k8s-p2-obs` → `deploy/k8s-p3-secrets` →
`deploy/k8s-p4-apps`.

---

## P1 — Kustomize platform (from production-deploy)

**Goal:** Codify [production-deploy.md](production-deploy.md) data-plane requirements as **Kustomize** under
[`deploy/platform/`](../deploy/platform/) — apply with `kubectl apply -k`, stable ClusterIP DNS in namespace
`fluxmeter`.

| Step | Work                                                                                                                                   | Acceptance                                                                                       |
|------|----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| P1.1 | **Redis** — 3 nodes (1 primary + 2 replicas), AOF (`appendonly yes` / `appendfsync everysec`), `noeviction`, Service `redis` → primary | Redis row in [k8s-verification.md](k8s-verification.md): `PING`, AOF on, 2 replicas              |
| P1.2 | **Kafka** — 3 brokers, bootstrap Service, topics `token-events` / `budget-alerts` / `token-events-dlq` (RF=3, `min.insync.replicas=2`) | Kafka row: 3/3 Ready, topics exist                                                               |
| P1.3 | **Flink** — install Operator; kind-sized `FlinkDeployment` (1 JM + 1 TM); args ready for platform DNS                                  | Flink Operator/REST healthy                                                                      |
| P1.4 | **ClickHouse** (optional baseline) — StatefulSet + init from `baseline/init.sql`, brokers = platform DNS                               | CH `SELECT 1` when enabled                                                                       |
| P1.5 | Top-level / overlay Kustomize (optional) — one `kubectl apply -k deploy/platform` entry that composes redis→kafka→flink→ch             | Documented apply order; Services resolve per [SERVICE-DNS.md](../deploy/platform/SERVICE-DNS.md) |

**Service types (kind):** ClusterIP (Redis, Kafka bootstrap, Flink REST).  
**Artifacts:** [`deploy/platform/{redis,kafka,flink,clickhouse}/`](../deploy/platform/).

**Non-goals for P1:** Grafana polish (→ P2), password Secrets (→ P3), app Helm (→ P4).

---

## P2 — Unified monitoring dashboards

**Goal:** One coherent observability surface for the P1 platform — operators see **health green** for Redis · Kafka ·
Flink · ClickHouse without hunting compose-only boards.

| Step | Work                                                                                                           | Acceptance                                                                      |
|------|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| P2.1 | Prometheus (or kube-prometheus stack) scrape targets for Redis / Kafka / Flink JM / CH                         | Targets `up==1` for in-scope components                                         |
| P2.2 | Grafana provisioning under `deploy/platform/observability/` (or reuse `grafana/provisioning` with kind values) | Single folder: **FluxMeter Platform**                                           |
| P2.3 | Unified dashboard rows: Redis (PING/mem/AOF), Kafka (brokers/ISR/lag), Flink (JM/TM/job), CH (queries/up)      | **Health green** for deployed rows ([k8s-verification.md](k8s-verification.md)) |
| P2.4 | Alert rules stub (critical: Redis down, Kafka under-replicated, Flink job missing) — fire only when broken     | Documented; no false criticals on idle healthy cluster                          |
| P2.5 | **Tracing** — Grafana **Tempo** (OTLP); Flink → Tempo via OTEL Java agent; Grafana Explore Tempo datasource     | No Jaeger; TraceQL finds `fluxmeter-flink` after Flink + Tempo Ready            |

**Artifacts:** [`deploy/platform/observability/`](../deploy/platform/observability/) (Prometheus + Grafana + **Tempo**) +
existing `grafana/` / `monitoring/`. Flink agent wiring: [`flink/flinkdeployment-kind.yaml`](../deploy/platform/flink/flinkdeployment-kind.yaml).

---

## P3 — Refine passwords (Secret stewardship)

**Goal:** Kind may keep empty/default passwords in ConfigMap for **local smoke only**. That pattern must **not** ship to
prod. All real credentials live in Kubernetes **Secret** (or Vault / external-secrets); platform and apps mount via
`envFrom` / projected volume / `users.d` — **never plaintext in ConfigMap or Git**.

### Kind vs prod

| Context | Allowed                                                                 | Forbidden                                      |
|---------|-------------------------------------------------------------------------|------------------------------------------------|
| **kind smoke** | Documented throwaway defaults (e.g. CH `default`/`default` in ConfigMap) | Copying that ConfigMap into prod overlays      |
| **prod / shared cluster** | Secret-held passwords; rotation; NetworkPolicy; TLS when exposed        | Passwords in Git, open `::/0` without policy   |

### Work items

| Step | Work                                                                                                                                 | Acceptance                                      |
|------|--------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|
| P3.1 | **Secret contract** — document names/keys: e.g. `fluxmeter-redis` (`REDIS_PASSWORD`), `fluxmeter-clickhouse` (`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`), `fluxmeter-grafana` (`GF_SECURITY_ADMIN_PASSWORD`) | README lists Secret names + keys                |
| P3.2 | **ClickHouse** — move credentials out of `users.xml` ConfigMap into Secret; mount as env or `users.d` file                           | No CH password in ConfigMap/Git for prod path   |
| P3.3 | **Redis / Grafana** — enable `requirepass` / admin auth; pods read password from Secret                                              | Same rule as CH                                 |
| P3.4 | **Bootstrap** — `kubectl create secret generic …` or kustomize `secretGenerator` with **gitignored** local overlays                  | Kind runbook; secrets never committed           |
| P3.5 | **Wire apps** — Helm/`FlinkDeployment` use `secretKeyRef` for `REDIS_PASSWORD` (and siblings); fail closed if missing                | App starts only when Secret present             |
| P3.6 | **Harden access** — rotate passwords; drop open networks (`::/0`) except via NetworkPolicy / private CIDR; prefer TLS on HTTP/native endpoints behind mesh/ingress when exposed beyond the cluster | Documented checklist; kind may defer TLS        |

**Non-goals for P3:** cloud KMS / Vault production install (note as follow-up after P4 smoke).

**CH pointer:** [deploy/platform/clickhouse/README.md](../deploy/platform/clickhouse/README.md) § Production hardening.

---

## P4 — App integration (stable usable service)

**Goal:** Turn the P1 data plane (+ P2 boards + P3 Secrets) into a **stable, usable FluxMeter service** on kind —
operators can hit API health, run a thin Full path (Kafka → Flink → Redis → API), and verify with one smoke checklist.
Compose (`make demo-full`) stays the laptop demo; P4 is the **K8s-shaped** path.

### What “stable usable” means

1. **Discoverable** — apps talk only to platform DNS (`redis.fluxmeter…`, `kafka-bootstrap.fluxmeter…`), not compose hostnames.
2. **Authenticated** — Redis/CH passwords from P3 Secrets; missing Secret → fail closed (no silent empty password).
3. **Observable** — P2 boards show API + Flink + Redis + Kafka green during/after smoke.
4. **Assertable** — small-N produce → expected Redis counters / API JSON ([k8s-verification.md](k8s-verification.md)).

### Workloads

| Workload                  | Image / artifact                | K8s form                                                   |
|---------------------------|---------------------------------|------------------------------------------------------------|
| **API**                   | `api/Dockerfile`                | Helm Deployment + Service (extend `deploy/helm/fluxmeter`) |
| **Streaming job**         | `shadowJar` / Flink image + jar | `FlinkDeployment` → Kafka + Redis DNS + Secret env         |
| **Webhook worker** (Full) | `api/Dockerfile.webhook`        | Deployment when Kafka alerts path is on                    |
| **Gateway** (optional)    | same API image                  | values toggle                                              |

### Work items

| Step | Work                                                                                                      | Acceptance                                                                     |
|------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| P4.1 | **Kind values profile** — `REDIS_HOST` / `KAFKA_BROKERS` / ports point at P1 Services                     | One file documents hosts (no compose names)                                    |
| P4.2 | **Secret wiring** — mount P3 Secrets; fail closed if missing                                              | `/health` covers Redis auth path                                               |
| P4.3 | **Deploy API** — Helm install/upgrade against platform; port-forward or ClusterIP probe                   | `GET /health` ok                                                               |
| P4.4 | **Deploy Flink job** — tiny parallelism; Kafka→Redis; jar/`kind load` documented                          | Job `RUNNING`; small-N events land in Redis + API                              |
| P4.5 | **End-to-end smoke** — produce N events → wait window SLA → assert counters                               | `k8s-smoke` pass ([k8s-verification.md](k8s-verification.md))                  |
| P4.6 | **Runbook** — kind → P1 apply -k → P2 obs → P3 secrets → P4 helm/job → verify                             | Copy-paste path; dashboard green + small-N                                     |
| P4.7 | **Prod overlay notes** — swap DNS for MSK/ElastiCache; S3 checkpoints; scale TM — **same chart keys**     | Documented only (optional apply later)                                         |

**Non-goals for P4:** 100K eps, Redis 6-node cluster client, multi-AZ, Intelligence control-plane charts.

---

## Tracking

| Phase                 | Status                                                                    |
|-----------------------|---------------------------------------------------------------------------|
| P1 Kustomize platform | **In progress** — Redis 3-node HA + AOF; Kafka/Flink/CH kustomize present |
| P2 Unified monitoring | **In progress** — Prometheus + Grafana + exporters under `deploy/platform/observability/` |
| P3 Refine passwords   | Not started                                                               |
| P4 App integration    | Not started (minimal API chart exists as starting point)                  |

---

## Out of scope (unless demand)

- 1M eps on kind
- Intelligence / control-plane dedicated charts
- Terraform/AWS until P4 smoke is real (then optional IaC for MSK/EKS)

---

## Indexes

| Doc                                                       | Role                                                          |
|-----------------------------------------------------------|---------------------------------------------------------------|
| [k8s-verification.md](k8s-verification.md)                | Single `k8s-smoke` checklist + health green + small-N asserts |
| [deploy/platform/README.md](../deploy/platform/README.md) | P1 how-to                                                     |
| [deploy/helm/README.md](../deploy/helm/README.md)         | App chart install (P4)                                        |
| [production-deploy.md](production-deploy.md)              | Prod sizing & checklist                                       |
| [disaster-recovery.md](disaster-recovery.md)              | DR                                                            |
