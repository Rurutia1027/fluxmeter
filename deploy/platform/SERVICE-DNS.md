# Service DNS & exposure conventions

All platform components live in namespace **`fluxmeter`** unless noted.

Kubernetes default DNS suffix: **`svc.cluster.local`**.

## Stable names (use these in Flink args / API env)

| Role                     | Service name                          | DNS                                               | Port                       | Service type (kind)                  | Service type (prod note)                      |
|--------------------------|---------------------------------------|---------------------------------------------------|----------------------------|--------------------------------------|-----------------------------------------------|
| Redis                    | `redis`                               | `redis.fluxmeter.svc.cluster.local`               | 6379                       | **ClusterIP**                        | ClusterIP behind mesh/LB; or managed endpoint |
| Kafka bootstrap          | `kafka-bootstrap`                     | `kafka-bootstrap.fluxmeter.svc.cluster.local`     | 9092                       | **ClusterIP**                        | NLB / MSK bootstrap string                    |
| Kafka brokers (headless) | `kafka-brokers`                       | `kafka-brokers.fluxmeter.svc.cluster.local`       | 9092                       | **Headless** (`clusterIP: None`)     | Same pattern for StatefulSet                  |
| Flink REST (JM)          | set by Operator (often `<name>-rest`) | e.g. `fluxmeter-rest.fluxmeter.svc.cluster.local` | 8081                       | **ClusterIP**                        | Ingress / internal only                       |
| ClickHouse               | `clickhouse`                          | `clickhouse.fluxmeter.svc.cluster.local`          | 8123 (HTTP), 9000 (native) | **ClusterIP**                        | Optional baseline; not billing SoR            |
| Grafana          | `grafana`                             | `grafana.fluxmeter.svc.cluster.local`             | 3000                       | **NodePort 30300** on kind           | Ingress + auth                                |
| Prometheus       | `prometheus`                          | `prometheus.fluxmeter.svc.cluster.local`          | 9090                       | **ClusterIP**                        | optional remote-write                         |
| Tempo (traces)   | `tempo`                               | `tempo.fluxmeter.svc.cluster.local`               | 3200, **4317** (OTLP gRPC), 4318 | **ClusterIP**                   | same; no Jaeger — Grafana Explore → Tempo     |

## Why ClusterIP first

- Flink TaskManagers, API, and Kafka clients talk **in-cluster**; no need for LoadBalancer on kind.
- External laptop access: `kubectl port-forward svc/redis 6379:6379 -n fluxmeter` (same for Grafana / Flink UI).
- Prod: keep ClusterIP; put AWS NLB / Ingress only on API/Grafana (and MSK/ElastiCache replace in-cluster Kafka/Redis).

## Wire into Flink / API

```text
--KAFKA_BROKERS=kafka-bootstrap.fluxmeter.svc.cluster.local:9092
--REDIS_HOST=redis.fluxmeter.svc.cluster.local
```

Short names also work **inside** the same namespace: `kafka-bootstrap:9092`, `redis`.

## Labels (for ServiceMonitor / Grafana discovery later)

```yaml
metadata:
  labels:
    app.kubernetes.io/part-of: fluxmeter
    app.kubernetes.io/component: redis|kafka|flink|clickhouse|grafana
```
