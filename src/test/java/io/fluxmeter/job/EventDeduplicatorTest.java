package io.fluxmeter.job;

import io.fluxmeter.model.TokenEvent;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventDeduplicatorTest {
    @Test
    void shouldDripDuplicateEvent() throws Exception {
        EventDeduplicator deduplicator = new EventDeduplicator();
        KeyedProcessOperator<String, TokenEvent, TokenEvent> operator =
                new KeyedProcessOperator<>(deduplicator);
        KeyedOneInputStreamOperatorTestHarness<String, TokenEvent, TokenEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        TokenEvent::getEventId,
                        Types.STRING);

        harness.open();

        String eventId = UUID.randomUUID().toString();

        // events contains duplicated event ids
        List<TokenEvent> output = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            harness.processElement(genTokenEventWithCustomerId(eventId, "cust_" +  i), i);
        }

        harness.getOutput().forEach(record -> {
            @SuppressWarnings("unchecked")
            StreamRecord<TokenEvent> streamRecord =
                    (StreamRecord<TokenEvent>) record;
            output.add(streamRecord.getValue());
        });

        // no duplicated items should appear in output item list
        assertEquals(1, output.size());

        // element remained in output should be the first item
        assertEquals("cust_0", output.get(0).getCustomerId());
        assertEquals(eventId, output.get(0).getEventId());

        harness.close();
    }

    @Test
    void shouldEmitDuplicateEventAfterTtlExpires() throws Exception {
        EventDeduplicator eventDeduplicator = new EventDeduplicator();
        KeyedProcessOperator<String, TokenEvent, TokenEvent> operator =
                new KeyedProcessOperator<>(eventDeduplicator);
        KeyedOneInputStreamOperatorTestHarness<String, TokenEvent, TokenEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        TokenEvent::getEventId,
                        Types.STRING
                );
        harness.open();

        // State TTL clock is MockTtlTimeProvider - advance via setStateTtlProcessingTime,
        // NOT setProcessingTime (that only drives timers)
        long ttlMs = Time.hours(1).toMilliseconds();
        harness.setStateTtlProcessingTime(0L);

        String eventId = UUID.randomUUID().toString();
        TokenEvent event1 = genTokenEventWithCustomerId(eventId, "cust_1");
        TokenEvent event2 = genTokenEventWithCustomerId(eventId, "cust_2");

        // event 1 kept; see-state written at ttl-time = 0
        harness.processElement(event1, 0);

        // advance TTL clock past 1h -> seen-state for this eventId is expired/cleared
        harness.setStateTtlProcessingTime(ttlMs + 1);

        // same eventId after TTL -> 2 is kept (emitted), not filtered
        harness.processElement(event2, 0);

        List<TokenEvent> out = new ArrayList<>();
        harness.getOutput().forEach(record -> {
            @SuppressWarnings("unchecked")
            StreamRecord<TokenEvent> streamRecord = (StreamRecord<TokenEvent>) record;
            out.add(streamRecord.getValue());
        });

        // event1 was already emitted earlier; after TTL clears state, event2 should be kept
        assertEquals(2, out.size());
        assertEquals("cust_1", out.get(0).getCustomerId());
        assertEquals("cust_2", out.get(1).getCustomerId());

        // event1 & event2 event id should be equal
        assertEquals(event1.getEventId(), event2.getEventId());

        harness.close();
    }

    private TokenEvent genTokenEvent(String eventId) {
        TokenEvent event = new TokenEvent();
        event.setEventId(eventId);
        event.setCustomerId("cust_1");
        event.setModelId("gpt-4o");
        return event;
    }

    private TokenEvent genTokenEventWithCustomerId(String eventId, String customerId) {
        TokenEvent event = genTokenEvent(eventId);
        event.setCustomerId(customerId);
        return event;
    }
}