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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Layer B: real Redis via Testcontainers (aligned with compose {@code redis:7-alpine}.
 *
 * <p>Verifies SET NX + counters are atomic: second apply of the same window is SKIP.
 * Opt out without Docker: {@code FLUXMETER_SKIP_TESTCONTAINERS=1}.
 */

@Testcontainers
@EnabledIf("io.fluxmeter.sink.RedisSinkIdempotencyTest#testcontainersEnabled")
class RedisSinkIdempotencyTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort());

    /**
     * Used by {@link EnabledIf}; must stay public for Junit discovery.
     */
    public static boolean testcontainersEnabled() {
        return !"1".equals(System.getenv("FLUXMETER_SKIP_TESTCONTAINERS"));
    }

    @BeforeAll
    static void requireDocker() {
        Assumptions.assumeTrue(
                testcontainersEnabled(),
                "FLUXMETER_SKIP_TESTCONTAINERS=1 - skipping Redis Testcontainers");
    }

    @BeforeEach
    void loadCatalog() throws Exception {
        byte [] json = Files.readAllBytes(Path.of("config/pricing.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void secondApplyOfSameWindowIsSkipped() {
        String host = REDIS.getHost();
        int port = REDIS.getMappedPort(6379);

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(2);
        cfg.setMaxWait(Duration.ofSeconds(2));

        try (JedisPool pool = new JedisPool(cfg, host, port, 2000)) {
            String customerId = "idempotency_test_" + System.currentTimeMillis();
            long windowStart = 1_700_000_000L;
            long windowEnd = windowStart + 10_000L;

            TokenEvent event = new TokenEvent();
            event.setEventId("evt-idem-" + customerId);
            event.setCustomerId(customerId);
            event.setModelId("gpt-4o-mini");
            event.setProvider("openai");
            event.setInputTokens(1_000);
            event.setOutputTokens(0);
            event.setTimestamp(windowStart + 1);

            UsageAggregate agg = new UsageAggregate(customerId, "gpt-4o-mini",
                    windowStart, windowEnd);
            agg.addEvent(event);

            try (Jedis jedis = pool.getResource()) {
                String customerKey = TenantKeys.customerPrefix(null, customerId);
                String idempotencyKey = "applied:" + TenantKeys.windowId(
                        null, customerId, "gpt-4o-mini", windowStart);
                String first = RedisSink.apply(jedis, agg);
                assertEquals("OK", first);

                long inputAfterFirst = Long.parseLong(jedis.get(customerKey + ":input_tokens"));
                assertEquals(1000L, inputAfterFirst);

                String second = RedisSink.apply(jedis, agg);
                assertEquals("SKIP", second);

                long inputAfterSecond = Long.parseLong(jedis.get(customerKey +
                        ":input_tokens"));
                assertEquals(1000L, inputAfterSecond, "replay must not double-count");

                jedis.del(
                        idempotencyKey,
                        customerKey + ":input_tokens",
                        customerKey + ":output_tokens",
                        customerKey + ":total_tokens",
                        customerKey + ":cost_usd",
                        customerKey + ":event_count");
            }
        }
    }
}
