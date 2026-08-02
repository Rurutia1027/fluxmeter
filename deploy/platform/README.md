# Platform stack (WIP) — Redis · Kafka · Flink on kind

Infra-first data plane for Full-mode FluxMeter on Kubernetes.

**Plan:** [docs/k8s-deployment-plan.md](../../docs/k8s-deployment-plan.md) (phases **P1 → P4**).  
**Prod requirements:** [docs/production-deploy.md](../../docs/production-deploy.md).

```text
P1  Kustomize platform     ← this directory (apply -k)
P2  Unified monitoring     observability/
P3  Secret stewardship     Secret-held passwords
P4  App integration        Helm API + Flink job on platform DNS
```

## P1 apply order (do not skip)

1. **Redis** — 3-node HA, AOF, `noeviction`
2. **Kafka** — 3 brokers, RF=3 topics
3. **Flink Operator** — then a kind-sized `FlinkDeployment`
4. **ClickHouse** (optional baseline) — same Kafka topic, store-then-query lag vs Flink ([clickhouse/](clickhouse/))

| Dir                              | Focus                                                   | In-cluster DNS (namespace `fluxmeter`)                      | Prod-grade yet?                        |
|----------------------------------|---------------------------------------------------------|-------------------------------------------------------------|----------------------------------------|
| [redis/](redis/)                 | 3-node HA + AOF + ClusterIP                             | `redis.fluxmeter.svc.cluster.local:6379` (primary)          | **Partial** — 1 primary + 2 replicas   |
| [kafka/](kafka/)                 | 3× KRaft `apache/kafka:3.7.0` (compose-aligned)         | `kafka-bootstrap.fluxmeter.svc.cluster.local:9092`          | Kustomize WIP                          |
| [flink/](flink/)                 | Operator + JM/TM                                        | `fluxmeter-rest.fluxmeter.svc.cluster.local:8081` (typical) | **No** — skeleton                      |
| [clickhouse/](clickhouse/)       | Baseline `clickhouse/clickhouse-server:24.1` + init Job | `clickhouse.fluxmeter.svc.cluster.local:8123`               | Kustomize ready                        |
| [observability/](observability/) | Grafana + Prometheus + **Tempo** + exporters (**P2**)   | `grafana…:3000` · `tempo…:4317` (OTLP) · NodePort 30300     | **Ready** — Health + Tempo (no Jaeger) |

DNS / Service types: [SERVICE-DNS.md](SERVICE-DNS.md).

## Kind constraints (32GB Mac)

| Component  | Prod (`production-deploy.md`)        | This WIP on kind                                    |
|------------|--------------------------------------|-----------------------------------------------------|
| Redis      | 3 primary + 3 replica cluster        | **3** StatefulSet (1 primary + 2 replicas, AOF)     |
| Kafka      | 3+ brokers, RF=3                     | **3**× `apache/kafka:3.7.0` KRaft (Kustomize)       |
| Flink      | 4× TM, parallelism 8, S3 checkpoints | Operator + **1 JM + 1 TM**, local checkpoints       |
| ClickHouse | Optional baseline / analytics        | **1** replica, 2–4Gi; init from `baseline/init.sql` |
| Grafana    | Full ops                             | **P2** — NodePort 30300 + Platform Health board     |
| Secrets    | Secret-held passwords                | **P3**                                              |
| App        | Stable API + Flink path              | **P4**                                              |

Give Docker Desktop **≥10GB**; do not run `make demo-full` and this stack at the same time.

## Quick start (P1 outline)

```bash
kubectl create namespace fluxmeter

# P1 — kustomize platform
kubectl apply -k deploy/platform/redis/
kubectl apply -k deploy/platform/kafka/      # see kafka/README.md
kubectl apply -k deploy/platform/flink/      # see flink/README.md
kubectl apply -k deploy/platform/clickhouse/ # optional

kubectl get svc -n fluxmeter

# P2 — unified monitoring (after Redis/Kafka up)
kubectl apply -k deploy/platform/observability/
# Grafana: http://localhost:30300  (admin / fluxmeter)
# Dashboard: FluxMeter Platform Health — Redis/Kafka/Flink/CH UP tiles
# Traces: Explore → Tempo → {resource.service.name="fluxmeter-flink"}

# P3 — create Secrets (when ready), then remount platform/apps
# P4 — helm upgrade … + Flink job against platform DNS
```

Status: **WIP** — P1 in progress; P2–P4 not production-hardened.
