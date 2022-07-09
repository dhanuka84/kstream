package io.confluent.developer;

import static org.junit.Assert.assertEquals;


import io.confluent.developer.avro.UserEvent;
import io.confluent.developer.avro.UserRollup;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;


public class CogroupingStreamsTest {

    private final static String TEST_CONFIG_FILE = "configuration/test.properties";

    @Test
    public void cogroupingTest() throws IOException {
        final CogroupingStreams instance = new CogroupingStreams();
        final Properties allProps = instance.loadEnvProperties(TEST_CONFIG_FILE);

        final String appOneInputTopicName = allProps.getProperty("app-one.topic.name");
        final String appTwoInputTopicName = allProps.getProperty("app-two.topic.name");
        final String appThreeInputTopicName = allProps.getProperty("app-three.topic.name");
        final String totalResultOutputTopicName = allProps.getProperty("output.topic.name");

        final Topology topology = instance.buildTopology(allProps);
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, allProps)) {

            final Serde<String> stringAvroSerde = Serdes.String();
            final SpecificAvroSerde<UserEvent> loginEventSerde = CogroupingStreams.getSpecificAvroSerde(allProps);
            final SpecificAvroSerde<UserRollup> rollupSerde = CogroupingStreams.getSpecificAvroSerde(allProps);

            final Serializer<String> keySerializer = stringAvroSerde.serializer();
            final Deserializer<String> keyDeserializer = stringAvroSerde.deserializer();
            final Serializer<UserEvent> loginEventSerializer = loginEventSerde.serializer();


            final TestInputTopic<String, UserEvent>  appOneInputTopic = testDriver.createInputTopic(appOneInputTopicName, keySerializer, loginEventSerializer);
            final TestInputTopic<String, UserEvent>  appTwoInputTopic = testDriver.createInputTopic(appTwoInputTopicName, keySerializer, loginEventSerializer);
            final TestInputTopic<String, UserEvent>  appThreeInputTopic = testDriver.createInputTopic(appThreeInputTopicName, keySerializer, loginEventSerializer);

            final TestOutputTopic<String, UserRollup> outputTopic = testDriver.createOutputTopic(totalResultOutputTopicName, keyDeserializer, rollupSerde.deserializer());


            final List<UserEvent> appOneEvents = new ArrayList<>();
            appOneEvents.add(UserEvent.newBuilder().setEventId("one").setUserId("foo").setTime(5L).build());
            appOneEvents.add(UserEvent.newBuilder().setEventId("one").setUserId("bar").setTime(6l).build());
            appOneEvents.add(UserEvent.newBuilder().setEventId("one").setUserId("bar").setTime(7L).build());

            final List<UserEvent> appTwoEvents = new ArrayList<>();
            appTwoEvents.add(UserEvent.newBuilder().setEventId("two").setUserId("foo").setTime(5L).build());
            appTwoEvents.add(UserEvent.newBuilder().setEventId("two").setUserId("foo").setTime(6l).build());
            appTwoEvents.add(UserEvent.newBuilder().setEventId("two").setUserId("bar").setTime(7L).build());

            final List<UserEvent> appThreeEvents = new ArrayList<>();
            appThreeEvents.add(UserEvent.newBuilder().setEventId("three").setUserId("foo").setTime(5L).build());
            appThreeEvents.add(UserEvent.newBuilder().setEventId("three").setUserId("foo").setTime(6l).build());
            appThreeEvents.add(UserEvent.newBuilder().setEventId("three").setUserId("bar").setTime(7L).build());
            appThreeEvents.add(UserEvent.newBuilder().setEventId("three").setUserId("bar").setTime(9L).build());

            final Map<String, Map<String, Long>> expectedEventRollups = new TreeMap<>();
            
            final UserRollup expectedUserRollup = new UserRollup(expectedEventRollups);
            
            final Map<String, Long> expectedAppOneRollup = new HashMap<>();
            expectedAppOneRollup.put("three", 2L);
            expectedAppOneRollup.put("two", 1L);
            expectedAppOneRollup.put("one", 2L);
            expectedEventRollups.put("bar", expectedAppOneRollup);

            final Map<String, Long> expectedAppTwoRollup = new HashMap<>();
            expectedAppTwoRollup.put("three", 2L);
            expectedAppTwoRollup.put("two", 2L);
            expectedAppTwoRollup.put("one", 1L);
            expectedEventRollups.put("foo", expectedAppTwoRollup);

			/*
			 * final Map<String, Long> expectedAppThreeRollup = new HashMap<>();
			 * expectedAppThreeRollup.put("foo", 2L); expectedAppThreeRollup.put("bar", 2L);
			 * expectedEventRollups.put("three", expectedAppThreeRollup);
			 */

            sendEvents(appOneEvents, appOneInputTopic);
            sendEvents(appTwoEvents, appTwoInputTopic);
            sendEvents(appThreeEvents, appThreeInputTopic);

            final List<UserRollup> actualUserEventResults = outputTopic.readValuesToList();
            final Map<String, Map<String, Long>> actualRollupMap = new HashMap<>();
            for (UserRollup actualUserEventResult : actualUserEventResults) {
                  actualRollupMap.putAll(actualUserEventResult.getEventByUser());
            }
            final UserRollup actualUserRollup = new UserRollup(actualRollupMap);

            assertEquals(expectedUserRollup, actualUserRollup);
        }
    }


    private void sendEvents(List<UserEvent> events, TestInputTopic<String, UserEvent> testInputTopic) {
        for (UserEvent event : events) {
             testInputTopic.pipeInput(event.getUserId(), event);
        }
    }
}
