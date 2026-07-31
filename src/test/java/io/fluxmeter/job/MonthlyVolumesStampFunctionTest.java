package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Layer A: {@link MonthlyVolumeStampFunction} ValueState - stamps
 * {@code _monthlyVolumeBefore} per keyed event and advances the meter.
 *
 * <p>No windowing, out-of-order merge, or USD math here. For each arriving event:
 * - First, load the monthly meter from ValueState (keyed by customer|model if no tenant
 *   exists) stamp {@code tokensBefore} into metadata,
 * - Then, advance and save the meter.
 * <p>
 * Tier/plan rates live in {@link io.fluxmeter.pricing.PricingCatalog}; cost is applied
 * later with {@link UsageAggregateFunction} (@see
 * {@link UsageAggregateFunctionPipelineTest}).
 */
class MonthlyVolumesStampFunctionTest {
    private static final String BEFORE_KEY = UsageAggregateFunction.MONTHLY_VOLUME_BEFORE_KEY;

    @BeforeEach
    void clearSink() {
        CollectSink.values.clear();
    }

    // Carve the token stream at tier boundaries; pricing comes later.
    @Test
    void valueStateAccumulatesBeforeStampPerKey() throws Exception {
        runStamp(List.of(
                event("e1", "cust_a", "gpt-4o-mini", ts("2026-07-04T12:00:00Z"), 100),
                event("e2", "cust_a", "gpt-4o-mini", ts("2026-07-04T12:01:00Z"), 50),
                event("e3", "cust_a", "gpt-4o-mini", ts("2026-07-04T12:02:00Z"), 25)
        ));

        List<TokenEvent> stamped = forCustomer("cust_a");

        // {e1, e2, e3} exist in stamped
        assertEquals(3, stamped.size());

        //  0         100        150        175
        // ├─────────┼──────────┼──────────┤
        // │   e1    │    e2    │    e3    │
        // │  (100)  │   (50)   │   (25)   │
        // │ before=0│ before=100│before=150│

        assertEquals("0", stamped.get(0).getMetadata().get(BEFORE_KEY));
        assertEquals("100", stamped.get(1).getMetadata().get(BEFORE_KEY));
        assertEquals("150", stamped.get(2).getMetadata().get(BEFORE_KEY));
    }

    @Test
    void valueStateIsIsolatedAcrossAggregationKeys() throws Exception {
        runStamp(List.of(
                event("a1", "cust_a", "gpt-4o-mini", ts("2026-07-04T12:00:00Z"), 1_000),
                event("b1", "cust_b", "gpt-4o-mini", ts("2026-07-04T12:00:00Z"), 500),
                event("a2", "cust_a", "gpt-4o-mini", ts("2026-07-04T12:01:00Z"), 10),
                event("b2", "cust_b", "gpt-4o-mini", ts("2026-07-04T12:01:00Z"), 20)
        ));

        Map<String, String> a2 = byId("a2").getMetadata();
        Map<String, String> b2 = byId("b2").getMetadata();

        // a1 before key = 0, a2 before key is a1's token cnt = 1_000
        assertEquals("1000", a2.get(BEFORE_KEY));

        // b1 before key = 0, b2 before key is b1's token cnt = 500
        assertEquals("500", b2.get(BEFORE_KEY));
    }

    @Test
    void valueStateResetsWhenUtcMonthRolls() throws Exception {
        runStamp(List.of(
                event("july", "cust_roll", "gpt-4o-mini", ts("2026-07-31T23:59:00Z"), 5_000_000),
                event("aug", "cust_roll", "gpt-4o-mini", ts("2026-08-01T00:01:00Z"), 100)
        ));

        // month rolls result in before key reset to zero
        assertEquals("0", byId("july").getMetadata().get(BEFORE_KEY));
        // August sees a fresh meter — July's 5M must not carry over
        assertEquals("0", byId("aug").getMetadata().get(BEFORE_KEY));
    }

    // --- private classes & functions ---
    private static void runStamp(List<TokenEvent> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TokenEvent> stream = env.addSource(new ListEventSource(events));
        stream
                // key -> customerId + "|" + modelId;
                .keyBy(TokenEvent::getAggregationKey)
                .process(new MonthlyVolumeStampFunction())
                .addSink(new CollectSink());

        // local fake flink job entry point
        env.execute("monthly-volume-stamp-function-test");
    }

    // extract list of events from sink result collections by providing customerId
    private static List<TokenEvent> forCustomer(String customerId) {
        return CollectSink.values.stream()
                .filter(e -> customerId.equals(e.getCustomerId()))
                .sorted(Comparator.comparingLong(TokenEvent::getTimestamp))
                .toList();
    }

    // extract list of events from sink result collections by providing eventId
    private static TokenEvent byId(String eventId) {
        return CollectSink.values.stream()
                .filter(e -> eventId.equals(e.getEventId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing event " + eventId
                        + " in " + CollectSink.values.stream().map(TokenEvent::getEventId).toList()));
    }

    private static long ts(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    private static TokenEvent event(String id, String customerId, String modelId, long ts,
                                    int inputTokens) {
        TokenEvent e = new TokenEvent();
        e.setEventId(id);
        e.setCustomerId(customerId);
        e.setModelId(modelId);
        e.setProvider("openai");
        e.setTimestamp(ts);
        e.setInputTokens(inputTokens);
        e.setOutputTokens(0);
        assertNotNull(e.getAggregationKey());
        return e;
    }


    @SuppressWarnings("deprecation")
    private static class ListEventSource implements SourceFunction<TokenEvent> {
        private final List<TokenEvent> events;
        private volatile boolean running = true;

        public ListEventSource(List<TokenEvent> events) {
            this.events = events;
        }

        @Override
        public void run(SourceContext<TokenEvent> ctx) throws Exception {
            for (TokenEvent e : events) {
                if (!running) {
                    return;
                }

                // emit event to mock flink event stream
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(e);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class CollectSink implements SinkFunction<TokenEvent> {
        static final List<TokenEvent> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(TokenEvent value, Context context) throws Exception {
            values.add(value);
        }
    }
}
