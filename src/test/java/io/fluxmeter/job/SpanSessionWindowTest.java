package io.fluxmeter.job;

import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.model.TokenEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Session-window span attribution (60s gap), matching TokenUsageAggregator.
 */
class SpanSessionWindowTest {

    private static final long GAP_MS = 60_000L;

    @BeforeEach
    void clearSink() {
        CollectSink.values.clear();
    }

    @Test
    void eventsWithinGapStayInOneSessionWindow() throws Exception {
        // 0s and 30s apart (< 60s gap) → one span fire after watermark past last+gap
        runPipeline(List.of(
                event("e1", "span_a", 0L, 100, 0, "gpt-4o-mini"),
                event("e2", "span_a", 30_000L, 50, 10, "gpt-4o"),
                // closer: advances WM past session end (30_000 + 60_000)
                closer("close", 90_001L)
        ));

        assertEquals(1, CollectSink.values.size());
        SpanAggregate span = CollectSink.values.get(0);
        assertEquals("span_a", span.getSpanId());
        assertEquals(2, span.getCallCount());
        assertEquals(160, span.getTotalTokens());
        assertEquals(0L, span.getFirstEventTime());
        assertEquals(30_000L, span.getLastEventTime());
        assertEquals(30_000L, span.getDurationMs());
        assertTrue(span.getCostUsd() > 0.0);
    }

    @Test
    void gapOverSixtySecondsSplitsIntoTwoSessionWindows() throws Exception {
        // 0s then 70s → gap > 60s → two sessions for same parentSpanId
        runPipeline(List.of(
                event("e1", "span_b", 0L, 10, 0, "gpt-4o-mini"),
                event("e2", "span_b", 70_000L, 20, 0, "gpt-4o-mini"),
                closer("close", 70_000L + GAP_MS + 1)
        ));

        assertEquals(2, CollectSink.values.size());
        List<SpanAggregate> ordered = new ArrayList<>(CollectSink.values);
        ordered.sort(Comparator.comparingLong(SpanAggregate::getFirstEventTime));

        assertEquals(1, ordered.get(0).getCallCount());
        assertEquals(0L, ordered.get(0).getFirstEventTime());
        assertEquals(0L, ordered.get(0).getLastEventTime());

        assertEquals(1, ordered.get(1).getCallCount());
        assertEquals(70_000L, ordered.get(1).getFirstEventTime());
        assertEquals(70_000L, ordered.get(1).getLastEventTime());
    }

    @Test
    void differentParentSpansAggregateIndependently() throws Exception {
        runPipeline(List.of(
                event("e1", "span_x", 0L, 10, 0, "gpt-4o-mini"),
                event("e2", "span_y", 1_000L, 20, 0, "gpt-4o-mini"),
                event("e3", "span_x", 2_000L, 30, 0, "claude-sonnet-4"),
                closer("close", 2_000L + GAP_MS + 1)
        ));

        assertEquals(2, CollectSink.values.size());
        SpanAggregate x = bySpanId("span_x");
        SpanAggregate y = bySpanId("span_y");

        assertEquals(2, x.getCallCount());
        assertEquals(40, x.getTotalTokens());
        assertEquals(0L, x.getFirstEventTime());
        assertEquals(2_000L, x.getLastEventTime());

        assertEquals(1, y.getCallCount());
        assertEquals(20, y.getTotalTokens());
    }

    @Test
    void fiveCallsAcrossModelsRollUpToOneSpan() throws Exception {
        // TEST_PLAN §7 — same parentSpanId, mixed models, one session
        List<TokenEvent> events = new ArrayList<>();
        String[] models = {"gpt-4o-mini", "gpt-4o", "claude-sonnet-4", "gpt-4o-mini", "gpt-4o"};
        long t = 0L;
        for (int i = 0; i < 5; i++) {
            events.add(event("e" + i, "agent_run_1", t, 100 + i, 10, models[i]));
            t += 10_000L; // 10s apart, well under 60s gap
        }
        events.add(closer("close", t - 10_000L + GAP_MS + 1));

        runPipeline(events);

        assertEquals(1, CollectSink.values.size());
        SpanAggregate span = CollectSink.values.get(0);
        assertEquals("agent_run_1", span.getSpanId());
        assertEquals(5, span.getCallCount());
        // inputs 100..104 + 5*10 output = 560
        assertEquals(560, span.getTotalTokens());
        assertEquals(0L, span.getFirstEventTime());
        assertEquals(40_000L, span.getLastEventTime());
        assertEquals(40_000L, span.getDurationMs());
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

        stream
                .filter(e -> e.getParentSpanId() != null && !e.getParentSpanId().isEmpty())
                .keyBy(TokenEvent::getParentSpanId)
                .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                .aggregate(new TokenUsageAggregator.SpanAggregateFunction())
                .addSink(new CollectSink());

        env.execute("span-session-window-test");
    }

    private static SpanAggregate bySpanId(String spanId) {
        return CollectSink.values.stream()
                .filter(s -> spanId.equals(s.getSpanId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing span " + spanId));
    }

    private static TokenEvent event(
            String id, String parentSpanId, long ts, int input, int output, String model) {
        TokenEvent e = new TokenEvent();
        e.setEventId(id);
        e.setCustomerId("cust_span");
        e.setParentSpanId(parentSpanId);
        e.setModelId(model);
        e.setProvider("openai");
        e.setTimestamp(ts);
        e.setInputTokens(input);
        e.setOutputTokens(output);
        return e;
    }

    /** No parentSpanId — filtered from aggregation; used only to push watermark. */
    private static TokenEvent closer(String id, long ts) {
        TokenEvent e = new TokenEvent();
        e.setEventId(id);
        e.setCustomerId("cust_span");
        e.setModelId("gpt-4o-mini");
        e.setProvider("openai");
        e.setTimestamp(ts);
        e.setInputTokens(0);
        return e;
    }

    /** Emits watermark at closer event timestamp so session windows can fire. */
    private static final class CloserWatermarkGenerator
            implements WatermarkGenerator<TokenEvent>, Serializable {
        @Override
        public void onEvent(TokenEvent event, long eventTimestamp, WatermarkOutput output) {
            if (event.getEventId() != null && event.getEventId().startsWith("close")) {
                output.emitWatermark(new Watermark(eventTimestamp));
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
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class CollectSink implements SinkFunction<SpanAggregate> {
        static final List<SpanAggregate> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(SpanAggregate value, Context context) {
            values.add(value);
        }
    }
}
