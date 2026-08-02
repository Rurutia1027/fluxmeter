# ClickHouse (kind) — baseline / store-then-query

Adapted
from [Shortlink-GitOps clickhouse base](https://github.com/Rurutia1027/Shortlink-GitOps/tree/06d984e638ca6aa312620fcc32830fc7d23fb873/k8s/base/clickhouse):
StatefulSet + Service + init Job pattern. Dropped shortlink DDL, Argo hooks, log-viewer, large PVCs.

**Image:** `clickhouse/clickhouse-server:24.1` (same as `docker-compose.full.yml`)  
**DNS:** `clickhouse.fluxmeter.svc.cluster.local:8123` (HTTP) / `:9000` (native)  
**Auth (kind):** user `default` / password `default` (non-empty for IDE clients; still in ConfigMap — prod → Secret)  
**DDL:** FluxMeter [`baseline/init.sql`](../../../baseline/init.sql) with  
`kafka_broker_list = kafka-bootstrap.fluxmeter.svc.cluster.local:9092`

## Start

```bash
# Image (Hub flaky → mirror then tag)
docker pull clickhouse/clickhouse-server:24.1
# or: docker pull docker.m.daocloud.io/clickhouse/clickhouse-server:24.1
#     docker tag docker.m.daocloud.io/clickhouse/clickhouse-server:24.1 clickhouse/clickhouse-server:24.1
kind load docker-image clickhouse/clickhouse-server:24.1 --name fluxmeter

kubectl apply -k deploy/platform/clickhouse/
kubectl -n fluxmeter rollout status sts/clickhouse --timeout=300s

# Baseline tables + Kafka engine (needs Kafka up)
kubectl delete job -n fluxmeter clickhouse-init --ignore-not-found
kubectl apply -f deploy/platform/clickhouse/init-job.yaml
kubectl -n fluxmeter wait --for=condition=complete job/clickhouse-init --timeout=180s
```

## Verify

```bash
kubectl -n fluxmeter exec sts/clickhouse -- \
  clickhouse-client --user default --password default -q "SELECT 1"
kubectl -n fluxmeter exec sts/clickhouse -- \
  clickhouse-client --user default --password default -q "SHOW TABLES FROM fluxmeter"
```

Local UI (after port-forward): host `localhost`, HTTP `8123` / native `9000`, DB `fluxmeter`, user/password *
*`default` / `default`**.

## Production hardening (P3 — refine passwords)

Kind keeps password plaintext in ConfigMap (`users.xml`) for **local smoke only**. **Do not ship that pattern to
prod.**

| Do                                                                                         | Don't                                      |
|--------------------------------------------------------------------------------------------|--------------------------------------------|
| Store CH credentials in a **Kubernetes Secret** (or Vault / external-secrets)              | Leave passwords in ConfigMap / Git         |
| Mount as env or `users.d` file into the StatefulSet                                        | Bake prod passwords into image layers      |
| Rotate passwords; restrict `networks` — no open `::/0` except via NetworkPolicy / private CIDR | Expose HTTP/native to the world unauthenticated |
| Prefer TLS on HTTP (`8123`) / native (`9000`) behind mesh or ingress when exposed beyond the cluster | Assume ClusterIP alone is enough for multi-tenant prod |

Kind may keep `default`/`default` until P3 lands; prod overlays must switch to Secret-held credentials before any
shared/staging use.

See plan: [k8s-deployment-plan.md](../../../docs/k8s-deployment-plan.md) § P3.

## Removed vs Shortlink

- `clickhouse-log-viewer` debug pod
- Argo CD PostSync annotations
- shortlink_stats / link_stats_* DDL
- 100Gi+20Gi disks → **20Gi** data PVC; kind-sized CPU/RAM
- `23.12-alpine` → **24.1**
