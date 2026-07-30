package io.fluxmeter.job;

import io.fluxmeter.model.SpanAggregate;
import io.fluxmeter.model.TokenEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
        // 0s and 30s apart (< 60s gap) -> one span fire after watermark past last+gap
        runPipeline(List.of(
                // 2 events share same parent-span-id && ts range < 60s gap,
                // they should be located in the same window
                event("e1", "span_a", 0L, 100, 0, "gpt-4o-mini"),
                event("e2", "span_a", 30_000L, 50, 10, "gpt-4o"),
                // trigger of watermark close window and executes aggregation inner window,
                // finally each window --> 1 CollectSink#values' 1 record
                closer("close", 90_001L)
        ));

        // e1 & e2 share same keyId,and timestamp range locates
        assertEquals(1, CollectSink.values.size());
        SpanAggregate span = CollectSink.values.get(0);
        assertEquals("span_a", span.getSpanId());
        assertEquals(2, span.getCallCount());// e1 & e2 add two events

        // e1: {input: 100}, e2: {input: 50, output: 10} -> 160
        assertEquals(160, span.getTotalTokens());

        // e1: [0,0] & e2: [0, 30_000]
        assertEquals(0L, span.getFirstEventTime());
        assertEquals(30_000L, span.getLastEventTime());

        assertEquals(30_000L, span.getDurationMs());
        assertTrue(span.getCostUsd() > 0);
    }

    @Test
    void gapOverSixtySecondsSplitsIntoTwoSessionWindows() throws Exception {
        // e1#timestamp and e2#timestamp's gap = 70s > window gap 60s
        // which means flink operator receives the first event then in the coming 60s  =
        // window gap no new coming events
        // so , even though both of those two events shares same keyId(parent span id)
        // e1, e2 will be arranged into two session windows, this can be verified via final
        // sink array list size

        runPipeline(List.of(
                event("e1", "span_b", 0L, 10, 0, "gpt-4o-mini"),
                event("e2", "span_b", 70_000L, 20, 0, "gpt-4o-mini"),
                // trigger of close watermark
                closer("close", 70_000L + GAP_MS + 1)
        ));

        assertEquals(2, CollectSink.values.size());
        List<SpanAggregate> ordered = new ArrayList<>(CollectSink.values);
        // sort final records via first event time field --> [e1, e2]
        ordered.sort(Comparator.comparingLong(SpanAggregate::getFirstEventTime));

        // should be 1, in session window 1 only e1 is added
        assertEquals(1, ordered.get(0).getCallCount());

        // e1 both first & last event time is 0
        assertEquals(0L, ordered.get(0).getFirstEventTime());
        assertEquals(0L, ordered.get(0).getLastEventTime());


        // should be 2, in session window 2 only e2 is added
        assertEquals(1, ordered.get(1).getCallCount());

        // e2 both first & last event time is 70_000
        assertEquals(70_000L, ordered.get(1).getFirstEventTime());
        assertEquals(70_000L, ordered.get(1).getLastEventTime());
    }


    @Test
    void differentParentSpansAggregateIndependently() throws Exception {
        runPipeline(List.of(
                event("e1", "span_x", 0L, 10, 0, "gpt-4o-mini"),
                event("e2", "span_y", 1_000L, 20, 10, "gpt-4o-mini"),
                event("e3", "span_x", 2_000L, 30, 20, "claude-sonnet-4"),
                closer("close", 2_000L + GAP_MS + 1)
        ));

        // e1, e3 -> {span_x, [e1,e3]} -> sink
        // e2 -> {span_y, [e2]} -> sink
        // sink records -> 2

        assertEquals(2, CollectSink.values.size());
        SpanAggregate x = bySpanId("span_x");
        SpanAggregate y = bySpanId("span_y");

        assertEquals(2, x.getCallCount()); // e1, e3 addEvents twice
        assertEquals(60, x.getTotalTokens());
        assertEquals(0L, x.getFirstEventTime());
        assertEquals(2_000L, x.getLastEventTime());

        assertEquals(1, y.getCallCount()); // e2
        assertEquals(30, y.getTotalTokens());
    }

    @Test
    void fiveCallAcrossModelRollUpToOneSpan() throws Exception {
        // same parentSpanId, mixed models, one session
        List<TokenEvent> events = new ArrayList<>();
        String[] models = {"gpt-4o-mini", "gpt-4o", "claude-sonnet-4", "gpt-4o-mini", "gpt-4o"};
        long t = 0L;
        for (int i = 0; i < 5; i++) {
            events.add(event("e" + i, "agent_run_1", t, 100 + i, 10, models[i]));
            t += 10_000L;  // 10s apart, well under 60s gap
        }

        // finally append close signal event
        events.add(closer("close", t - 10_000L + GAP_MS + 1));

        runPipeline(events);

        // our operator will organize events into same window, when
        // 1. they come with timestamp gap < GAP_MS (60s)
        // 2. share the same parent span id

        // {e0 ... e4} -> same window with key = "agent_run1" --> 1 sink record
        assertEquals(1, CollectSink.values.size());
        SpanAggregate span = CollectSink.values.get(0);
        assertEquals("agent_run_1", span.getSpanId());
        assertEquals(5, span.getCallCount());

        // inputs 100...104 + 5 * 10 output = 560 total tokens
        assertEquals(560, span.getTotalTokens());
        assertEquals(0L, span.getFirstEventTime());
        assertEquals(40_000L, span.getLastEventTime());
        assertEquals(40_000L, span.getDurationMs());
    }


    // --- private helper funcs ---
    private static void runPipeline(List<TokenEvent> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);

        // create data stream in type of TokenEvent
        DataStream<TokenEvent> stream = env
                .fromCollection(events, TypeInformation.of(TokenEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TokenEvent>forGenerator(ctx -> new CloserWatermarkGenerator())
                                // let TokenEvent#timeStamp be the event time
                                // session window will be organized by this ts field
                                .withTimestampAssigner((e, ts) -> e.getTimestamp()));


        stream
                // first, filter items which conditions not satisfied
                .filter(e -> e.getParentSpanId() != null && !e.getParentSpanId().isEmpty())
                // second, extract parentSpanId, into <parentSpanId:String,event:TokenEvent> kv pairs
                .keyBy(TokenEvent::getParentSpanId)
                // third, create window with gap = 60s, during this period
                // all items with same key value will be collected in corresponding buffer space
                .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                // fourth, execute aggregate, all records with each timestamp in window range
                // and share the same key(parent span id) will be collected as the rules described
                // in TokenUsageAggregator#SpanAggregateFunc()
                .aggregate(new TokenUsageAggregator.SpanAggregateFunction())
                // last, collected each window session generated aggregated results
                // and sync to local variable the synchronized array list
                .addSink(new CollectSink());

        // entry point of local 'faked' flink app
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

    /**
     * No parentSpanId - filtered from aggregation; used only to push watermark
     */
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

    /**
     * Emits watermark at closer event timestamp so session windows can fire.
     */
    private static class CloserWatermarkGenerator implements WatermarkGenerator<TokenEvent>,
            Serializable {
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

    private static class CollectSink implements SinkFunction<SpanAggregate> {
        static final List<SpanAggregate> values =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(SpanAggregate value, Context context) throws Exception {
            values.add(value);
        }
    }
}
