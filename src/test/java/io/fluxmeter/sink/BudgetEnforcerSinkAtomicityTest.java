package io.fluxmeter.sink;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.pricing.PricingCatalog;
import io.fluxmeter.util.TenantKeys;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Layer B: BudgetEnforcerSink Lua - counters + budget deduct + SET NX atomicity.
 *
 * <p>Kafka alert emission is out of scope (Redis path only via
 * {@link BudgetEnforcerSink#apply}.
 * Opt out without Docker: {@code FLUXMETER_SKIP_TESTCONTAINERS=1}.
 * <p>
 * 1 session window -> 1 sink -> 1 atomic write via LUA EVAL to Redis;
 * every atomic op return value in {SKIP / NONE / OK / LOW / EXHAUSTED} which represents
 * different semantics
 * <p>
 * ---
 * LUA EVAL return: [0] = status,  SKIP | NONE | OK | EXHAUSTED
 * [1] = newBalance, [2] = balance before deduct (string)
 */

@Testcontainers
@EnabledIf("io.fluxmeter.sink.BudgetEnforcerSinkAtomicityTest#testcontainersEnabled")
class BudgetEnforcerSinkAtomicityTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort());

    /**
     * Used by {@link EnabledIf}; must stay public for JUnit discovery.
     */
    public static boolean testcontainersEnabled() {
        return !"1".equals(System.getenv("FLUXMETER_SKIP_TESTCONTAINERS"));
    }

    @BeforeAll
    static void requireDocker() {
        Assumptions.assumeTrue(
                testcontainersEnabled(),
                "FLUXMETER_SKIP_TESTCONTAINERS=1 — skipping BudgetEnforcer Testcontainers");
    }

    @BeforeEach
    void loadCatalog() throws Exception {
        byte[] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void deductBudgetAndSkipsReplay() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            String customerId = String.format("budget_ok_" + System.currentTimeMillis());
            String budgetKey = TenantKeys.budgetPrefix(null, customerId);
            String customerKey = TenantKeys.customerPrefix(null, customerId);

            jedis.set(budgetKey + ":balance_usd", "1.0");
            jedis.set(budgetKey + ":initial_balance_usd", "1.0");

            UsageAggregate agg = aggregate(customerId, 1_000);
            double cost = agg.getCostUsd();
            assertTrue(cost > 0, "pricing catalog must yield positive cost");

            List<String> first = BudgetEnforcerSink.apply(jedis, agg);
            assertEquals("OK", first.get(0));
            assertEquals(1.0 - cost, Double.parseDouble(first.get(1)), 1e-9);
            assertEquals(1000L, Long.parseLong(jedis.get(customerKey + ":input_tokens")));

            // duplicated commit && idempotency key will be rejected this operation
            // redis inner value will not be not double-counted
            List<String> second = BudgetEnforcerSink.apply(jedis, agg);
            assertEquals("SKIP", second.get(0));
            assertEquals(1.0 - cost,
                    Double.parseDouble(jedis.get(budgetKey + ":balance_usd")), 1e-9);
            assertEquals(1000L, Long.parseLong(jedis.get(customerKey + ":input_tokens")));
        }
    }

    @Test
    void exhaustsBudgetAndRecordsDebt() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            String customerId = String.format("budget_ex_", System.currentTimeMillis());
            String budgetKey = TenantKeys.budgetPrefix(null, customerId);

            // create usage agg instance by passing input total token cnt
            // and aggregate create agg instance based on event with model = 'gpt-4o-mini'
            // total cost usd via agg should approximately ~ $0.15 > $0.01
            UsageAggregate agg = aggregate(customerId, 1_000_000); // ~$0.15 for gpt-4o-mini
            double cost = agg.getCostUsd();
            assertTrue(cost > 0.01, "need enough cost to exhaust tiny balance");

            jedis.set(budgetKey + ":balance_usd", "0.01");
            jedis.set(budgetKey + ":initial_balance_usd", "0.01");

            List<String> result = BudgetEnforcerSink.apply(jedis, agg);

            // since balance deduce request ~ $0.15, and account remaining usd ~ $0.0
            // expected ret from result should be 'EXHAUSTED' which means no extra balance
            // under customer id account
            // [0] = status, request cost > balanced -> EXHAUSTED
            assertEquals("EXHAUSTED", result.get(0));

            // [1] = newBalance, -> no extra balance --> 0.0
            assertEquals(0.0, Double.parseDouble(result.get(1)), 1e-9);

            // balance_usd ran out of usage
            assertEquals(0.0, Double.parseDouble(jedis.get(budgetKey + ":balance_usd")), 1e-9);

            // debt_usd = request cost - balance_usd
            assertEquals(cost - 0.01, Double.parseDouble(jedis.get(budgetKey + ":debt_usd"))
                    , 1e-6);
        }
    }

    @Test
    void noBudgetKeyReturnsNoneButStillWritesCounters() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            jedis.flushDB(); // isolate global:* from sibling tests on shared container

            String customerId = "budget_none_" + System.currentTimeMillis();
            String customerKey = TenantKeys.customerPrefix(null, customerId);
            String modelKey = customerKey + ":model:gpt-4o-mini";
            String budgetKey = TenantKeys.budgetPrefix(null, customerId);
            String windowId = TenantKeys.windowId(null, customerId, "gpt-4o-mini", 1_700_000_000_000L);

            UsageAggregate agg = aggregate(customerId, 1_000);
            List<String> result = BudgetEnforcerSink.apply(jedis, agg);
            assertEquals("NONE", result.get(0));

            // Usage side written before the balance check
            assertEquals("1", jedis.get("applied:" + windowId));
            assertEquals("1000", jedis.get(customerKey + ":input_tokens"));
            assertEquals("0", jedis.get(customerKey + ":output_tokens"));
            assertEquals("1000", jedis.get(customerKey + ":total_tokens"));
            assertEquals("1", jedis.get(customerKey + ":event_count"));
            assertEquals(agg.getCostUsd(), Double.parseDouble(jedis.get(customerKey + ":cost_usd")), 1e-9);
            assertEquals("1000", jedis.get(modelKey + ":input_tokens"));
            assertEquals(agg.getCostUsd(), Double.parseDouble(jedis.get(modelKey + ":cost_usd")), 1e-9);
            assertEquals("1000", jedis.get(TenantKeys.globalKey(null, "input_tokens")));
            assertEquals("1000", jedis.get(TenantKeys.globalKey(null, "total_tokens")));
            assertEquals("1", jedis.get(TenantKeys.globalKey(null, "total_events")));
            assertEquals(agg.getCostUsd(),
                    Double.parseDouble(jedis.get(TenantKeys.globalKey(null, "total_cost_usd"))), 1e-9);
            assertEquals(String.valueOf(agg.getWindowEnd()),
                    jedis.get(TenantKeys.globalKey(null, "last_window_end")));
            // Script increments deducted before returning NONE
            assertEquals(agg.getCostUsd(),
                    Double.parseDouble(jedis.get(budgetKey + ":total_deducted_usd")), 1e-9);

            // Budget keys that are only read / written on deduct path - absent
            assertNull(jedis.get(budgetKey + ":balance_usd"));
            assertNull(jedis.get(budgetKey + ":initial_balance_usd"));
            assertNull(jedis.get(budgetKey + ":alert_threshold_usd"));
            assertNull(jedis.get(budgetKey + ":debt_usd"));
        }
    }

    // -- private methods --
    private static JedisPool pool() {
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(2);
        cfg.setMaxWait(Duration.ofSeconds(2));
        return new JedisPool(cfg, REDIS.getHost(), REDIS.getMappedPort(6379), 2000);
    }

    private static UsageAggregate aggregate(String customerId, int inputTokens) {
        long windowStart = 1_700_000_000_000L;
        long windowEnd = windowStart + 10_000L;

        TokenEvent event = new TokenEvent();
        event.setEventId("evt-budget-" + customerId);
        event.setCustomerId(customerId);
        event.setModelId("gpt-4o-mini");
        event.setProvider("openai");
        event.setInputTokens(inputTokens);
        event.setOutputTokens(0);
        event.setTimestamp(windowStart + 1);

        UsageAggregate agg = new UsageAggregate(customerId, "gpt-4o-mini", windowStart, windowEnd);
        agg.addEvent(event);
        return agg;
    }
}
