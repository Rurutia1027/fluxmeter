package io.fluxmeter.sink;

import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.util.TenantKeys;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
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

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Layer B: SpanSink Redis writes - SET overwrite (not INCR) + customer span ZSET.
 *
 * <p>Opt out without Docker: {@code FLUXMETER_SKIP_TESTCONTAINERS=1}.
 */
@Testcontainers
@EnabledIf("io.fluxmeter.sink.SpanSinkWriteTest#testcontainersEnabled")
public class SpanSinkWriteTest {
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
                "FLUXMETER_SKIP_TESTCONTAINERS=1 — skipping SpanSink Testcontainers");
    }

    @Test
    void writesSpanKeysAndCustomerZset() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            String customerId = String.format("cust_dur_%s", System.currentTimeMillis());
            String spanId = String.format("span_%s", System.currentTimeMillis());
            SpanAggregate span = span(customerId, spanId, 100, 0.05, 1_000L);

            assertTrue(SpanSink.apply(jedis, span));

            String key = "span:" + spanId;
            assertEquals("0.05", jedis.get(key + ":cost_usd"));
            assertEquals("100", jedis.get(key + ":total_tokens"));
            assertEquals("1", jedis.get(key + ":call_count"));
            assertEquals("0", jedis.get(key + ":duration_ms"));
            assertEquals(customerId, jedis.get(key + ":customer_id"));
            assertTrue(jedis.ttl(key + ":cost_usd") > 0);

            String zkey = TenantKeys.customerPrefix(null, customerId) + ":spans";
            assertEquals(Double.valueOf(0.05), jedis.zscore(zkey, spanId));
        }
    }

    @Test
    void recordsDurationAcrossCalls() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            String customerId = String.format("cust_dur_", System.currentTimeMillis());
            String spanId = String.format("span_dur_", System.currentTimeMillis() + 1);

            SpanAggregate span = span(customerId, spanId, 50, 0.01, 1_000L);
            TokenEvent late = new TokenEvent();
            late.setCustomerId(customerId);
            late.setParentSpanId(spanId);
            late.setInputTokens(50);
            late.setOutputTokens(0);
            late.setTimestamp(1_500L);
            span.addEvent(late, 0.01);

            SpanSink.apply(jedis, span);

            String key = "span:" + spanId;

            assertEquals("2", jedis.get(key + ":call_count"));
            assertEquals(span.getCallCount() + "", jedis.get(key + ":call_count"));


            assertEquals("500", jedis.get(key + ":duration_ms"));
            assertEquals(span.getDurationMs() + "", jedis.get(key + ":duration_ms"));

            assertEquals("100", jedis.get(key + ":total_tokens"));
            assertEquals(span.getTotalTokens() + "", jedis.get(key + ":total_tokens"));
        }
    }

    @Test
    void secondApplyOverwriteFullAggregate() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            String customerId = String.format("cust_ow_", System.currentTimeMillis());
            String spanId = String.format("span_ow", System.currentTimeMillis());

            SpanSink.apply(jedis, span(customerId, spanId, 100, 0.05, 1_000L));
            SpanSink.apply(jedis, span(customerId, spanId, 250, 0.12, 1_000L));

            String key = "span:" + spanId;
            assertEquals("0.12", jedis.get(key + ":cost_usd"));
            assertEquals("250", jedis.get(key + ":total_tokens"));
            assertEquals("1", jedis.get(key + ":call_count"));

            // ZADD updates score for same member - still one entry
            String zkey = TenantKeys.customerPrefix(null, customerId) + ":spans";
            assertEquals(1L, jedis.zcard(zkey));
            assertEquals(Double.valueOf(0.12), jedis.zscore(zkey, spanId));
        }
    }

    @Test
    void emptySpanIdIsNoOp() {
        try (JedisPool pool = pool(); Jedis jedis = pool.getResource()) {
            SpanAggregate empty = new SpanAggregate();
            empty.setCustomerId("c1");
            empty.setSpanId("");

            assertFalse(SpanSink.apply(jedis, empty));
            assertNull(jedis.get("span::cost_usd"));
        }
    }

    // -- private methods --
    private static JedisPool pool() {
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(2);
        cfg.setMaxWait(Duration.ofSeconds(2));
        return new JedisPool(cfg, REDIS.getHost(), REDIS.getMappedPort(6379), 2000);
    }

    private static SpanAggregate span(
            String customerId, String spanId, int inputTokens, double custUsd,
            long timestamp) {
        TokenEvent event = new TokenEvent();
        event.setCustomerId(customerId);
        event.setParentSpanId(spanId);
        event.setInputTokens(inputTokens);
        event.setOutputTokens(0);
        event.setTimestamp(timestamp);

        SpanAggregate span = new SpanAggregate();
        span.addEvent(event, custUsd);
        return span;
    }
}
