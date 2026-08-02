# Redis (WIP · kind → production-shaped)

**Prod ask** ([production-deploy.md](../../../docs/production-deploy.md)): Redis 7+, **AOF**, `noeviction`,
eventually cluster **3 primary + 3 replica** (or managed ElastiCache).

**This dir:** **3-node** StatefulSet — `redis-0` primary + `redis-1`/`redis-2` replicas — with AOF and the same
client DNS `redis.fluxmeter.svc.cluster.local:6379` (points at primary only, so existing `JedisPool` keeps working).

## Deploy style

|             | Kind (this dir)                                       | Full prod                               |
|-------------|-------------------------------------------------------|-----------------------------------------|
| Topology    | **3×** StatefulSet (1 primary + 2 replicas)           | Redis Cluster 3+3 or ElastiCache        |
| Service     | ClusterIP `redis` → `redis-0`; headless for replicaof | Same DNS name in front of managed Redis |
| Persistence | PVC 5Gi/pod, **AOF** (`appendonly yes`)               | Sized disks / managed                   |
| Policy      | `appendfsync everysec`, `maxmemory-policy noeviction` | Same                                    |

## Apply

```bash
kubectl apply -k deploy/platform/redis/
kubectl -n fluxmeter rollout status sts/redis
kubectl -n fluxmeter get pods -l app=redis
kubectl -n fluxmeter exec -it redis-0 -- redis-cli PING
kubectl -n fluxmeter exec -it redis-0 -- redis-cli INFO replication
```

Expect `role:master` and two connected replicas.

## Service type emphasis

- **ClusterIP `redis`** — write path for Flink sinks / API (`JedisPool`).
- **Headless `redis-headless`** — pod DNS for replication (`redis-N.redis-headless.fluxmeter.svc…`).
- Do not expose Redis NodePort/LoadBalancer on kind (billing data).

## Notes

- Previous WIP had `replicas: 1` and a broken AOF line (`appendonly yea` / `appendsync`). Fixed to real AOF directives.
- Scaling to true Redis Cluster (MOVED redirects) needs `JedisCluster` — out of scope until sinks are cluster-aware.
- Full **3+3** shard+replica topology remains the checklist item in `production-deploy.md` when >100K customers.
