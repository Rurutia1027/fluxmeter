# Observability (P2) — Prometheus + Grafana + Tempo

Unified monitoring for the kind platform. After Redis / Kafka / Flink / ClickHouse are up, apply this
overlay so Grafana **Platform Health** shows green UP tiles, and **Explore → Tempo** shows Flink traces.

**Stack choice:** metrics = Prometheus; traces = **Grafana Tempo** (OTLP). No Jaeger — keep one Grafana UX.

**Plan:** [k8s-deployment-plan.md](../../../docs/k8s-deployment-plan.md) § P2
**Verify:** [k8s-verification.md](../../../docs/k8s-verification.md)

## What you get

| Piece          | Role                                                           |
|----------------|----------------------------------------------------------------|
| Prometheus     | Scrapes exporters + Flink `:9249` + CH `:9363`                 |
| Tempo          | OTLP ingest (`:4317` gRPC / `:4318` HTTP); query `:3200`       |
| redis-exporter | `redis.fluxmeter.svc:6379` → job `redis`                       |
| kafka-exporter | `kafka-bootstrap.fluxmeter.svc:9092` → job `kafka`             |
| Grafana        | NodePort **30300**; datasources Prometheus + **Tempo** + Redis |
| Dashboards     | **FluxMeter Platform Health** (status) + metering (Redis keys) |

### Expected green after components start

| Tile       | Prometheus signal                              | Needs deployed                                       |
|------------|------------------------------------------------|------------------------------------------------------|
| Redis      | `up{job="redis"} == 1`                         | `sts/redis` + this overlay                           |
| Kafka      | `up{job="kafka"} == 1` (+ `kafka_brokers` ≥ 3) | `sts/kafka` + this overlay                           |
| Flink      | `sum(up{job="flink"}) >= 1`                    | Flink Operator + `FlinkDeployment` with metrics port |
| ClickHouse | `up{job="clickhouse"} == 1`                    | `sts/clickhouse` with `:9363`                        |

## Apply order

```bash
# 1) Platform (P1) — at least Redis + Kafka before exporters go green
kubectl apply -k deploy/platform/redis/
kubectl apply -k deploy/platform/kafka/
# optional:
kubectl apply -k deploy/platform/clickhouse/
kubectl apply -k deploy/platform/flink/   # after Operator install — see flink/README.md

# 2) Observability (P2)
kubectl apply -k deploy/platform/observability/
kubectl -n fluxmeter rollout status deploy/prometheus
kubectl -n fluxmeter rollout status deploy/tempo
kubectl -n fluxmeter rollout status deploy/grafana
kubectl -n fluxmeter rollout status deploy/redis-exporter
kubectl -n fluxmeter rollout status deploy/kafka-exporter
```

## Open Grafana

```bash
# NodePort (kind / Docker Desktop)
open http://localhost:30300
# or: kubectl -n fluxmeter port-forward svc/grafana 3000:3000

# Login (kind smoke — move to Secret in P3)
# user: admin / password: fluxmeter
# anonymous Viewer enabled
```

Home dashboard: **FluxMeter Platform Health**.  
Metering board (Redis `global:*` keys): folder **FluxMeter Platform** → fluxmeter metering.

### Tracing (Tempo — no Jaeger)

1. Apply observability (Tempo + Grafana Tempo datasource) **before** or with Flink.
2. Flink JM/TM (`flinkdeployment-kind.yaml`) mount the OpenTelemetry Java agent and export OTLP →  
   `http://tempo.fluxmeter.svc.cluster.local:4317` (`service.name=fluxmeter-flink`).
3. In Grafana: **Explore** → datasource **Tempo** → Search / TraceQL, e.g.  
   `{resource.service.name="fluxmeter-flink"}`.

Flink **1.18** has no built-in `traces.reporter.otel` (that lives in Flink **2.x** `flink-metrics-otel`). The agent is
the kind-compatible equivalent of “image with tracing baked in”, without adding Jaeger to the stack.

## Quick PromQL checks

```bash
kubectl -n fluxmeter port-forward svc/prometheus 9090:9090
# Then:
# up{job="redis"}
# up{job="kafka"}
# kafka_brokers
# up{job="clickhouse"}
# up{job="flink"}
```

## Kind notes

- Grafana admin password is plaintext env for smoke only → **P3** moves to Secret.
- ClickHouse metrics need the `:9363` listen config (included in `clickhouse/configmap.yaml`); restart CH STS after
  upgrade if already running.
- Flink tiles stay red until Operator deploys JM/TM with Prometheus reporter (`flinkdeployment-kind.yaml`).
- Flink traces need Tempo Ready + Flink pods with the OTEL agent (initContainer pulls the jar on start).
- If Kafka is not up yet, `kafka-exporter` may crash-loop until brokers are Ready — re-apply after Kafka.

## DNS

| Service        | DNS                                          | Port                                                 |
|----------------|----------------------------------------------|------------------------------------------------------|
| grafana        | `grafana.fluxmeter.svc.cluster.local`        | 3000 (NodePort 30300)                                |
| prometheus     | `prometheus.fluxmeter.svc.cluster.local`     | 9090                                                 |
| tempo          | `tempo.fluxmeter.svc.cluster.local`          | 3200 (query), **4317** (OTLP gRPC), 4318 (OTLP HTTP) |
| redis-exporter | `redis-exporter.fluxmeter.svc.cluster.local` | 9121                                                 |
| kafka-exporter | `kafka-exporter.fluxmeter.svc.cluster.local` | 9308                                                 |
