package io.confluent.developer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import io.confluent.developer.avro.UserEvent;

public class EventTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return ((UserEvent)record.value()).getTime();
    }
}
