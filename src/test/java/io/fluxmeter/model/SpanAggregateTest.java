package io.fluxmeter.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit coverage for span window bounds (first/last event time + duration)
 * as used by session-window span attribution.
 */
class SpanAggregateTest {

    private static TokenEvent event(String parentSpanId, long timestamp, int input, int output) {
        TokenEvent e = new TokenEvent();
        e.setCustomerId("cust_1");
        e.setParentSpanId(parentSpanId);
        e.setTimestamp(timestamp);
        e.setInputTokens(input);
        e.setOutputTokens(output);
        return e;
    }

    @Test
    void firstEventSetsStartAndEnd() {
        SpanAggregate agg = new SpanAggregate();
        agg.addEvent(event("span_root", 1_000L, 10, 5), 0.01);

        assertEquals(1_000L, agg.getFirstEventTime());
        assertEquals(1_000L, agg.getLastEventTime());
        assertEquals(0L, agg.getDurationMs());
        assertEquals(1, agg.getCallCount());
        assertEquals(15, agg.getTotalTokens());
    }

    @Test
    void laterEventExpandsEndOnly() {
        SpanAggregate agg = new SpanAggregate();
        TokenEvent event1 = event("span_root", 1_000L, 10, 0);
        TokenEvent event2 = event("span_root", 1_500L, 20, 0);

        agg.addEvent(event1, 0.01);
        agg.addEvent(event2, 0.02);

        assertEquals(1_000L, agg.getFirstEventTime());
        assertEquals(1_500L, agg.getLastEventTime());
        assertEquals(500L, agg.getDurationMs());
        assertEquals(2, agg.getCallCount());
        assertEquals(30, agg.getTotalTokens());
    }

    @Test
    void earlierEventExpandsStartOnly() {
        SpanAggregate agg = new SpanAggregate();
        TokenEvent event1 = event("span_root", 2_000L, 10, 0);
        TokenEvent event2 = event("span_root", 1_200L, 5, 0);

        agg.addEvent(event1, 0.01);
        agg.addEvent(event2, 0.005);

        assertEquals(1_200L, agg.getFirstEventTime());
        assertEquals(2_000L, agg.getLastEventTime());
        assertEquals(800L, agg.getDurationMs());
    }

    @Test
    void midWindowEventDoesNotShrinkBounds() {
        SpanAggregate agg = new SpanAggregate();
        agg.addEvent(event("span_root", 1_000L, 1, 0), 0.001);
        agg.addEvent(event("span_root", 5_000L, 1, 0), 0.001);
        agg.addEvent(event("span_root", 2_000L, 1, 0), 0.001);

        assertEquals(1_000L, agg.getFirstEventTime());
        assertEquals(5_000L, agg.getLastEventTime());
        assertEquals(4_000L, agg.getDurationMs());
        assertEquals(3, agg.getCallCount());
    }

    @Test
    void sameTimestampDoesNotChangeWindowBounds() {
        SpanAggregate agg = new SpanAggregate();
        agg.addEvent(event("span_root", 2_000L, 10, 0), 0.01);
        agg.addEvent(event("span_root", 2_000L, 20, 5), 0.02);

        assertEquals(2_000L, agg.getFirstEventTime());
        assertEquals(2_000L, agg.getLastEventTime());
        assertEquals(0L, agg.getDurationMs());
        assertEquals(2, agg.getCallCount());
        assertEquals(35, agg.getTotalTokens());
    }

    @Test
    void mergeExpandsStartAndEndFromBothSides() {
        SpanAggregate a = new SpanAggregate();
        a.addEvent(event("span_root", 2_000L, 100, 0), 0.1);
        a.addEvent(event("span_root", 3_000L, 50, 0), 0.05);

        SpanAggregate b = new SpanAggregate();
        b.addEvent(event("span_root", 1_000L, 20, 0), 0.02);
        b.addEvent(event("span_root", 4_000L, 30, 0), 0.03);

        a.merge(b);

        assertEquals(1_000L, a.getFirstEventTime());
        assertEquals(4_000L, a.getLastEventTime());
        assertEquals(3_000L, a.getDurationMs());
        assertEquals(4, a.getCallCount());
        assertEquals(200, a.getInputTokens());
        assertEquals(0.2, a.getCostUsd());
    }

    @Test
    void mergeIntoEmptyTakesOtherWindowBounds() {
        SpanAggregate empty = new SpanAggregate();
        SpanAggregate other = new SpanAggregate();
        other.addEvent(event("span_root", 7_000L, 10, 0), 0.01);
        other.addEvent(event("span_root", 9_000L, 10, 0), 0.01);

        empty.merge(other);

        assertEquals(7_000L, empty.getFirstEventTime());
        assertEquals(9_000L, empty.getLastEventTime());
        assertEquals(2_000L, empty.getDurationMs());
        assertEquals(2, empty.getCallCount());
    }
}
