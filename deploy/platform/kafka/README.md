# Kafka (kind) — apache/kafka:3.7.0 KRaft × 3

**Aligned with** `docker-compose.full.yml` (`apache/kafka:3.7.0`, KRaft, **no ZooKeeper**).  
**Not** Bitnami / Confluent ZK. Helm Bitnami path removed.

DNS: `kafka-bootstrap.fluxmeter.svc.cluster.local:9092`  
Headless: `kafka-{0,1,2}.kafka-brokers.fluxmeter.svc.cluster.local`

## Image (manual pull if Hub is flaky)

```bash
docker pull apache/kafka:3.7.0
kind load docker-image apache/kafka:3.7.0 --name fluxmeter
```

Only this tag is required for brokers + topics Job.

## Start

```bash
# From repo root (namespace fluxmeter already exists from Redis)
kubectl apply -k deploy/platform/kafka/

kubectl -n fluxmeter rollout status sts/kafka --timeout=300s
kubectl -n fluxmeter get pods -l app=kafka

# Topics (RF=3) — after all 3 Ready
kubectl delete job -n fluxmeter kafka-topics-init --ignore-not-found
kubectl apply -f deploy/platform/kafka/topics-job.yaml
kubectl -n fluxmeter wait --for=condition=complete job/kafka-topics-init --timeout=180s
kubectl -n fluxmeter logs job/kafka-topics-init
```

## Verify

```bash
kubectl -n fluxmeter get svc | grep kafka
kubectl -n fluxmeter run kafka-cli --rm -it --restart=Never \
  --image=apache/kafka:3.7.0 --image-pull-policy=IfNotPresent -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-bootstrap.fluxmeter.svc.cluster.local:9092 --list
```

## Cleanup old Bitnami attempt (if still present)

```bash
helm uninstall kafka -n fluxmeter 2>/dev/null || true
kubectl -n fluxmeter delete sts kafka-controller --ignore-not-found
kubectl -n fluxmeter delete svc kafka kafka-controller-headless --ignore-not-found
kubectl -n fluxmeter delete pvc -l app.kubernetes.io/instance=kafka --ignore-not-found
```

## Kind memory

3× ~512Mi–1.5Gi heap/limit. Do not co-run `make demo-full`.
