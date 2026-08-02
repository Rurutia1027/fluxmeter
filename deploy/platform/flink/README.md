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

## 1. Install Operator

```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
# pin URL/version when leaving WIP — check current operator docs
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  -n flink-operator --create-namespace
```

Wait for Operator webhook/deployment Ready before applying `FlinkDeployment`.

## 2. RBAC + FlinkDeployment (1 JM + 1 TM)

```bash
kubectl apply -k deploy/platform/flink/
# or:
# kubectl apply -f deploy/platform/flink/rbac.yaml
# kubectl apply -f deploy/platform/flink/flinkdeployment-kind.yaml

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

## Service type emphasis

- REST **ClusterIP** — port-forward for humans; Ingress later.
- Do not put TM behind LoadBalancer.
- Prod keeps Operator; swap checkpoint dir to `s3://…` and bump TM replicas (see production-deploy.md).
