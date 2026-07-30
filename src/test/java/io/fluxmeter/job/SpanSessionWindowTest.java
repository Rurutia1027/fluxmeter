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
