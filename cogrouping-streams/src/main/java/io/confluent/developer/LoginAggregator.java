package io.confluent.developer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.kstream.Aggregator;

import io.confluent.developer.avro.UserEvent;
import io.confluent.developer.avro.UserRollup;

public class LoginAggregator implements Aggregator<String, UserEvent, UserRollup> {

  @Override
  public UserRollup apply(final String userId,
                           final UserEvent userEvent,
                           final UserRollup userRollup) {
    final String eventId = userEvent.getEventId();
    final Map<String, Map<String, Long>> userEvents = userRollup.getEventByUser();
    final Map<String, Long> userEventsCount = userEvents.computeIfAbsent(userId, key -> new HashMap<>());
    userEventsCount.compute(eventId, (k, v) -> v == null ? 1L : v + 1L);
    return userRollup;
  }
}
