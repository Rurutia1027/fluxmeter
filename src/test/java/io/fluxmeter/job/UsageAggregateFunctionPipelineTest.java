package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import io.fluxmeter.model.UsageAggregate;
import io.fluxmeter.pricing.PricingCatalog;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Layer A: Flink tumbling window + {@link UsageAggregateFunction}, matching production
 * pre-stamp via {@link MonthlyVolumeStampFunction}.
 *
 * <p>{@link UsageAggregateFunction} is an {@code AggregateFunction} (not a {@code
 * KeyedProcessFunction}); it only runs inside a window - same pattern as
 * {@link LateDataSideOutputTest}.
 *
 * <p>End-to-end pricing path: stamp supplies monthly volume {@code before} -> window
 * aggregates events -> {@link io.fluxmeter.pricing.PricingCatalog} turns volume tiered
 * tokens into USD (volume / graduate tiers). Cases cover summed windows, e2 split across
 * two events (meter continues), and graduated split across tier boundaries.
 */
class UsageAggregateFunctionPipelineTest {
    @BeforeEach
    void clearSinkAndLoadCatalog() throws Exception {
        CollectSink.values.clear();
        byte[] json = Files.readAllBytes(Path.of("contrib/pricing/tiered-example.json"));
        PricingCatalog.reload(PricingCatalog.loadFromBytes(json));
    }

    @Test
    void windowSumsTokensAndAttachesMetadata() throws Exception {
        runPipeline(List.of(
                event("e1", "cust_agg", "gpt-4o-mini", 1_000L, 100),
                event("e2", "cust_agg", "gpt-4o-mini", 2_000L, 50),
                // different key: advances watermark without joining the agg window
                closer("closer", "_wm", "gpt-4o-mini", 10_001L)
        ));


        UsageAggregate agg = onlyCustomer("cust_agg");
        assertEquals("gpt-4o-mini", agg.getModelId());
        assertEquals(150L, agg.getInputTokens());
        assertEquals(2L, agg.getEventCount());
        assertEquals(0L, agg.getWindowStart());
        assertEquals(10_000L, agg.getWindowEnd());
        assertTrue(agg.getCostMicro() > 0);
    }

    @Test
    void stampThenAggregate_secondEventUsesHigherVolumeTier() throws Exception {
        // volume tiers: [0,10M)=0.15, [10M,100M)=0.12 — same as UsageAggregateTierTest
        runPipeline(List.of(
                event("e1", "cust_tier", "gpt-4o-mini", 1_000L, 10_000_000),
                event("e2", "cust_tier", "gpt-4o-mini", 2_000L, 1_000_000),
                closer("closer", "_wm", "gpt-4o-mini", 10_001L)
        ));

        UsageAggregate agg = onlyCustomer("cust_tier");
        assertEquals(11_000_000L, agg.getInputTokens());
        // e1 at before=0: 10M * 0.15 = 1_500_000µ; e2 at before=10M: 1M * 0.12 = 120_000µ
        assertEquals(1_620_000L, agg.getCostMicro());
    }

    // model name: gpt-4o-mini decides PricingMode#VOLUME
    // coming append token will not be split, coming event#total_token only depends
    // previous token total cnt locates tier level
    @Test
    void stampThenAggregate_e2SplitIntoTwoContinuesMeter() throws Exception {
        // Same as higher-tier case, but e2 (1M) is split into e2a+e2b (500K + 500K)
        // Stamp ValueState must carry before across both halves
        runPipeline(List.of(
                event("e1", "cust_split", "gpt-4o-mini", 1_000L, 10_000_000),
                event("e2a", "cust_split", "gpt-4o-mini", 2_000L, 500_000),
                event("e2b", "cust_split", "gpt-4o-mini", 3_000L, 500_000),
                closer("closer", "_wm", "gpt-4o-mini", 10_001L)
        ));

        UsageAggregate agg = onlyCustomer("cust_split");
        assertEquals(11_000_000L, agg.getInputTokens());
        assertEquals(3L, agg.getEventCount());
        // e1: 10M*0.15=1_500_000µ; e2a at before=10M: 500k*0.12=60_000µ;
        // e2b at before=10.5M: 500k*0.12=60_000µ → same total as single e2
        assertEquals(1_620_000L, agg.getCostMicro());
    }

    // model name: claude-sonnet-4 decides PricingMode#GRADUATE
    // coming append token will be separate into different tiers and calculate
    @Test
    void stampThenAggregate_graduatedEventSplitsAcrossTiers() throws Exception {
        // claude-sonnet-4 graduated: first 1M tokens @ (2/4), rest @ (1/2) per million
        // Bring meter to 900k via e1, then e2 straddles the 1M boundary (same as
        // UsageAggregateTierTest)
        runPipeline(List.of(
                event("e1", "cust_grad", "claude-sonnet-4", 1_000L, 900_000),
                eventWithOutput("e2", "cust_grad", "claude-sonnet-4", 2_000L, 100_000, 100_000),
                closer("closer", "_wm", "claude-sonnet-4", 10_001L)
        ));

        UsageAggregate agg = onlyCustomer("cust_grad");
        assertEquals(1_000_000L, agg.getInputTokens());
        assertEquals(100_000L, agg.getOutputTokens());

        // e1 cost + e2 graduated split = matches in-JVM tier test for e2 alone (400_000µ)
        // plus e1: 900k input at first tier 2.00 -> 1_800_000µ
        assertEquals(1_800_000L + 400_000L, agg.getCostMicro());
    }

    // -- private classes & funcs ---
    // extract usage agg records from sink results by customer id
    private static UsageAggregate onlyCustomer(String customerId) {
        List<UsageAggregate> matched = CollectSink.values.stream()
                .filter(a -> customerId.equals(a.getCustomerId()))
                .toList();
        assertEquals(1, matched.size(), "aggregates=" + CollectSink.values);
        return matched.get(0);
    }

    private static void runPipeline(List<TokenEvent> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);

        DataStream<TokenEvent> stream = env
                .addSource(new ListEventSource(events))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TokenEvent>forGenerator(ctx -> new CloserWatermarkGenerator())
                                .withTimestampAssigner((e, ts) -> e.getTimestamp()));

        // Usage path (per customer|model):
        // 1) keyBy -> MonthlyVolumeStampFunction: ValueState meter; stamp tokensBefore
        //    into metadata (no USD yet); advance monthly volume
        // 2) keyBy -> tumbling window: same key + same fixed window slice
        // 3) UsageAggregateFunction: add/merge tokens + cost (catalog uses before)
        // 4) WindowMetadataFunction: fill tenant/customer/model from key and
        //    official windowStart/End (agg result lacks these; needed by sinks)
        // Late events (after watermark passes window end) --> side output / DLQ
        // No allowedLateness - avoids window re-fire vs SET NX idempotency.
        stream
                .keyBy(TokenEvent::getAggregationKey)
                .process(new MonthlyVolumeStampFunction())
                .keyBy(TokenEvent::getAggregationKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UsageAggregateFunction(), new TokenUsageAggregator.WindowMetadataFunction())
                .addSink(new CollectSink());

        env.execute("usage-aggregate-function-pipeline-test");
    }

    /**
     * Emits WM past the 10s window end when the closer event arrives.
     */
    private static final class CloserWatermarkGenerator
            implements WatermarkGenerator<TokenEvent>, Serializable {
        @Override
        public void onEvent(TokenEvent event, long eventTimestamp, WatermarkOutput output) {
            if (event.getEventId() != null && event.getEventId().startsWith("closer")) {
                output.emitWatermark(new Watermark(10_000));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // event-driven only
        }
    }

    @SuppressWarnings("deprecation")
    private static class ListEventSource implements SourceFunction<TokenEvent> {
        private final List<TokenEvent> events;
        private volatile boolean running = true;

        ListEventSource(List<TokenEvent> events) {
            this.events = events;
        }

        @Override
        public void run(SourceContext<TokenEvent> ctx) throws Exception {
            for (TokenEvent e : events) {
                if (!running) {
                    return;
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(e);
                }
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static TokenEvent event(
            String id, String customerId, String modelId, long ts, int inputTokens) {
        return eventWithOutput(id, customerId, modelId, ts, inputTokens, 0);
    }

    private static TokenEvent eventWithOutput(
            String id, String customerId, String modelId, long ts, int inputTokens, int outputTokens) {
        TokenEvent e = new TokenEvent();
        e.setEventId(id);
        e.setCustomerId(customerId);
        e.setModelId(modelId);
        e.setProvider("openai");
        e.setTimestamp(ts);
        e.setInputTokens(inputTokens);
        e.setOutputTokens(outputTokens);
        return e;
    }

    private static TokenEvent closer(String id, String customerId, String modelId, long ts) {
        return event(id, customerId, modelId, ts, 0);
    }

    private static class CollectSink implements SinkFunction<UsageAggregate> {
        static final List<UsageAggregate> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UsageAggregate value, Context context) {
            values.add(value);
        }
    }
}
