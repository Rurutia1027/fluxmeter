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
P3  Secret stewardship     Secret-held passwords; apps/platform mount envFrom / files
P4  App integration        API + Flink (+ gateway) on platform DNS → stable usable service
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

**Artifacts:** [`deploy/platform/observability/`](../deploy/platform/observability/) + existing `grafana/` /
`monitoring/`.

---

## P3 — Secret stewardship (passwords in Secret)

**Goal:** Stop plaintext / ad-hoc passwords. All sensitive credentials live in Kubernetes **Secret**; platform and apps
consume via `envFrom` / projected volume — ready for later SealedSecrets / external-secrets / Vault.

| Step | Work                                                                                                                    | Acceptance                          |
|------|-------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| P3.1 | Define Secret contract: e.g. `fluxmeter-redis`, `fluxmeter-clickhouse`, `fluxmeter-grafana` (keys documented)           | README lists Secret names + keys    |
| P3.2 | Redis (and CH/Grafana as applicable) enable auth; pods mount password from Secret                                       | No password in ConfigMap / git      |
| P3.3 | Example / generator: `kubectl create secret generic …` or kustomize `secretGenerator` with `.gitignore`d local overlays | Kind bootstrap documented           |
| P3.4 | Wire Helm/app values to `secretKeyRef` for `REDIS_PASSWORD` (and siblings)                                              | App starts only when Secret present |

**Non-goals for P3:** cloud KMS / Vault production install (document as next step after P4 smoke).

---

## P4 — App integration (stable usable service)

**Goal:** Run FluxMeter **apps** against the P1 platform (+ P2 boards + P3 Secrets) so the cluster provides a **stable,
usable** metering path — not just infra pods.

| Workload                  | Image / artifact                | K8s form                                                   |
|---------------------------|---------------------------------|------------------------------------------------------------|
| **API**                   | `api/Dockerfile`                | Helm Deployment + Service (extend `deploy/helm/fluxmeter`) |
| **Streaming job**         | `shadowJar` / Flink image + jar | `FlinkDeployment` → Kafka + Redis DNS + Secret env         |
| **Webhook worker** (Full) | `api/Dockerfile.webhook`        | Deployment when Kafka alerts path is on                    |
| **Gateway** (optional)    | same API image                  | values toggle                                              |

| Step | Work                                                                                             | Acceptance                                                                     |
|------|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| P4.1 | Chart/values use `redis.fluxmeter.svc.cluster.local` / `kafka-bootstrap…` (no compose hostnames) | One kind profile documents hosts/ports                                         |
| P4.2 | Mount P3 Secrets (`REDIS_PASSWORD`, …); fail closed if missing                                   | `/health` reflects Redis auth path                                             |
| P4.3 | Deploy API + tiny Flink job; produce small-N events → Redis counters / API JSON                  | `k8s-smoke` API + Flink rows pass ([k8s-verification.md](k8s-verification.md)) |
| P4.4 | Runbook: kind → P1 apply -k → P2 obs → P3 secrets → P4 helm/job → verification checklist         | Copy-paste path; dashboard green + small-N pass                                |
| P4.5 | Prod overlay notes: swap DNS for MSK/ElastiCache; S3 checkpoints; scale TM — same chart keys     | Documented only (optional apply later)                                         |

Compose (`make demo-full`) remains the heavy **laptop** Full demo; P4 is the **K8s-shaped** usable Full path.

---

## Tracking

| Phase                 | Status                                                                    |
|-----------------------|---------------------------------------------------------------------------|
| P1 Kustomize platform | **In progress** — Redis 3-node HA + AOF; Kafka/Flink/CH kustomize present |
| P2 Unified monitoring | Not started (compose Grafana exists; kind observability thin)             |
| P3 Secret stewardship | Not started                                                               |
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
