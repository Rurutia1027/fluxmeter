# Flink Operator + kind FlinkDeployment (WIP)

**Prod ask:** Flink Kubernetes Operator + RocksDB + S3 checkpoints + multi-TM.

**Kind focus:** install Operator once; run **1 JM + 1 TM**; checkpoints on emptyDir/PVC; clients reach Kafka/Redis via
`*.fluxmeter.svc.cluster.local`.

## Deploy style

| Piece           | How                                    | Service type                     |
|-----------------|----------------------------------------|----------------------------------|
| Flink Operator  | Helm (Apache Flink K8s Operator chart) | N/A (controllers)                |
| JobManager REST | Created by Operator (`*-rest`)         | **ClusterIP** `:8081`            |
| TaskManagers    | Operator-managed pods                  | talk to JM via headless/internal |

## One-shot (recommended)

From repo root (installs **cert-manager** if missing → **Operator 1.15.0** → `FlinkDeployment`):

```bash
chmod +x deploy/platform/flink/install.sh
./deploy/platform/flink/install.sh
```

> Chart pin **1.8.0 is gone** (404). Default is **1.15.0**. Override with `OPERATOR_VERSION=…`.

## 1. Install Operator (manual)

```bash
# Webhook needs cert-manager (skip if already installed)
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.18.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager --timeout=300s
kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s
kubectl -n cert-manager rollout status deploy/cert-manager-cainjector --timeout=300s

helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm repo update flink-operator-repo
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  -n flink-operator --create-namespace --wait --timeout 300s
```

Wait for Operator webhook/deployment Ready before applying `FlinkDeployment`.

## 2. RBAC + FlinkDeployment (1 JM + 1 TM)

```bash
kubectl create namespace fluxmeter   # if missing
kubectl apply -k deploy/platform/flink/

kubectl -n fluxmeter get flinkdeployment
kubectl -n fluxmeter get pods
kubectl -n fluxmeter get svc | grep rest
```

**Required:** `rbac.yaml` — JM uses SA `flink` to watch/create TaskManager pods. Without it you get
`HTTP 403 Forbidden` / ResourceManager fatal (not a real startup).

The `sed: Read-only file system` / `flink-conf.yaml` lines at boot are **noise** (Operator mounts conf read-only);
ignore if JM keeps running.

UI: `kubectl -n fluxmeter port-forward svc/fluxmeter-rest 8081:8081` → http://localhost:8081
(Adjust Service name to whatever the Operator creates for `metadata.name`.)

## 3. Wire brokers / Redis

Args in the kind Deployment use:

```text
kafka-bootstrap.fluxmeter.svc.cluster.local:9092
redis.fluxmeter.svc.cluster.local
```

Jar: start with **session mode / no jar** smoke (Operator healthy + blank cluster), then point `jarURI` at a PVC/
`kind load` image that contains `fluxmeter-*.jar` (K3 follow-up).

## Tracing → Grafana Tempo (no Jaeger)

Kind `FlinkDeployment` attaches the **OpenTelemetry Java agent** (initContainer download) and exports OTLP gRPC to:

```text
http://tempo.fluxmeter.svc.cluster.local:4317
```

Apply [`../observability/`](../observability/) so Tempo + Grafana Tempo datasource exist, then restart / re-apply Flink.
In Grafana: **Explore → Tempo** → `{resource.service.name="fluxmeter-flink"}`.

Flink 1.18 has no native `traces.reporter.otel`; Flink 2.x can use `flink-metrics-otel` instead of (or besides) the agent.

## Service type emphasis

- REST **ClusterIP** — port-forward for humans; Ingress later.
- Do not put TM behind LoadBalancer.
- Prod keeps Operator; swap checkpoint dir to `s3://…` and bump TM replicas (see production-deploy.md).
