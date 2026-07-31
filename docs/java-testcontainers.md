# Java Testcontainers — necessity & plan

**Status:** Proposal (review before implementation)  
**Date:** 2026-07-30  
**Scope:** Small-scale Java integration tests around the Full metering path (`src/test/java`)  
**Related:** [tests/TEST_PLAN.md](../tests/TEST_PLAN.md) · `make test-java` · `.github/workflows/gradle.yml`

---

## 1. Necessity (why now)

### Problem

Validating the Full path today tends to mean bringing up a **heavy local stack** (`docker-compose.full.yml`): Kafka,
Flink JM/TM, Redis, API, ...). That is the right tool for demos and deep E2E, but it is a poor fit for **day-to-day
regressions** after engine or sink changes:

1. **Startup cost is too high** - waiting for a full compose stack to become healthy dominates the feedback loop.
2. **In-process unit tests alone are not enough** - Flink operator tests (`LateDataSideOutputTest`, etc.) are fast and
   valuable, but they do **not** exercise real Redis Lua / `SET NX` semantics Kafka connector wiring, or other I/O
   boundaries where billing bugs hide.
3. **Code changes need a middle layer** - after touching sinks, aggregators, or serializers, we need a **small-scope
   integration suite** that covers a **thin but complete slice** of the chain (product -> process -> persist), without
   paying for a full cluster every time.

### Why Testcontainers (vs local Dockerfile / compose for this layer)

| Approach                                | Fit for small regression                                                                        |
|-----------------------------------------|-------------------------------------------------------------------------------------------------|
| Full `docker-compose.full.yml`          | Correct for E2E / load; too heavy for every Java change                                         |
| Mocks / skip-if-no-Redis                | Fast but **false green** in CI when Redis is absent (`RedisSinkIdempotencyTest` today)          |
| **Testcontainers (Redis, later Kafka)** | Ephemeral deps, JUnit lifecycle, **lighter than a full local Dockerfile stack**, natural for CI |

Testcontainers keep the **Flink job logic in-JVM** (MiniCluster / local `StreamExecutionEnvironment`) and only
containerizes **dependencies**. That is lighter than composing Flink itself in Docker for each test run, while still
hitting real Redis/Kafka behavior.

### Why it fits CI

- Same entrypoint as today: `./gradlew test` / `make test-java`
- GitHub-hosted runners already support Docker -> containers start on demand, tests fail or pass with a **normal JUnit
  report** (no separate "remember to start Redis" step).
- Suited to **narrow regression**: idempotency, sink automatically, optional Kafka source smoke - not a replacement for
  Python financial E2E.

**One-line necessity:** we need a **lightweight, CI-friendly, full-enough chain** between "pure UT" and "full compose,"
so engine changes get real I/O coverage without waiting on a full Flink stack every time.

---

## 2. What "complete chain" means here

Not the entire product demo. A **minimal vertical slice**:

```text
[in-JVM Flink operators / window]  +  [Testcontainers Redis (and later Kafka)]
        → assert counters / SET NX / deserialize+key  (Layer B)
```

```text
Layer A  Operator / window / watermark     → in-JVM Flink (keep as-is)
Layer B  Sink / Redis / Kafka I/O         → Testcontainers (add)  ← this proposal
Layer C  Full stack billing scenarios     → Python + compose (keep)
```

---

## 3. What we will and will not containerize 

### Use Testcontainers for
- **Redis** - highest value first (`RedisSink` / `BudgetEnforcerSink` Lua + idempotency)
- **Kafka** (phase2) - short connector / deserialize smoke if needed

### Do **not** run Flink JobManager / TaskManagers in Testcontainers for this suite 
- Prefer existing `flink-test-utils` / local env (see `LateDataSideOutputTest`)
- Faster, fewer flakes; Flink-in-Docker remains compose / K8s smoke, not every `./gradlew test`

---

## 4. Proposed phases (after approval)

### Phase 1 — Redis (minimal)

1. Add Testcontainers JUnit 5 dependencies to `build.gradle` (Java 17 / JUnit 5.10).
2. Migrate `RedisSinkIdempotencyTest` to `@Testcontainers` + `redis:7` (align tag with compose where practical).
3. Stop silent skip when Redis is down in CI; optional opt-out: `FLUXMETER_SKIP_TESTCONTAINERS=1` for machines without Docker.
4. Confirm `./gradlew test` on a Docker-capable runner always executes the idempotency assertion.
5. Optionally add one cheap `BudgetEnforcerSink` atomicity case on the same Redis container.

**Done when:** `make test-java` with Docker runs Redis idempotency; CI no longer false-greens by skipping it.

### Phase 2 — Kafka (optional)

1. `KafkaContainer` + `TokenEvent` JSON → source/deserialize/key smoke (still in-JVM Flink).
2. Avoid packaging a full JM/TM cluster inside the test.

### Phase 3 — CI wiring

1. Document Docker requirement for Layer B (this file + CONTRIBUTING if needed).
2. Ensure GitHub Actions Java job has Docker and fails if Phase 1 tests are skipped unexpectedly.
3. Do **not** replace `tests/test_integration.py` with Testcontainers.

---

## 5. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Slower `./gradlew test` | Containers only for I/O tests; reuse one Redis per test class |
| No Docker on some laptops | Opt-out env; Layer A tests still run |
| Image pull flakes | Pin Redis tag; rely on runner cache |
| Over-weight Flink-in-Docker | Explicit non-goal (§3) |

---

## 6. Out of scope

- GHCR / image publish
- Helm / K8s test jobs
- Replacing Python E2E
- Java / Go metering SDKs

---

## 7. Review checklist

- [ ] Agree: main driver is **faster small-chain regression** vs full compose startup
- [ ] Agree: Layer A in-JVM; Layer B Testcontainers; Layer C compose E2E
- [ ] Approve Phase 1 scope (Redis idempotency first)
- [ ] Confirm CI may require Docker for the full Java suite
- [ ] Then implement Phase 1 incrementally

**Next step after approval:** Phase 1 only — deps + migrate `RedisSinkIdempotencyTest`.


