package io.confluent.developer;


import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;

import io.confluent.developer.avro.UserEvent;
import io.confluent.developer.avro.UserRollup;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class CogroupingStreams {


    public Topology buildTopology(Properties allProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String appOneInputTopic = allProps.getProperty("app-one.topic.name");
        final String appTwoInputTopic = allProps.getProperty("app-two.topic.name");
        final String appThreeInputTopic = allProps.getProperty("app-three.topic.name");
        final String totalResultOutputTopic = allProps.getProperty("output.topic.name");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<UserEvent> loginEventSerde = getSpecificAvroSerde(allProps);
        final Serde<UserRollup> loginRollupSerde = getSpecificAvroSerde(allProps);


        final KStream<String, UserEvent> appOneStream = builder.stream(appOneInputTopic, Consumed.with(stringSerde, loginEventSerde));
        final KStream<String, UserEvent> appTwoStream = builder.stream(appTwoInputTopic, Consumed.with(stringSerde, loginEventSerde));
        final KStream<String, UserEvent> appThreeStream = builder.stream(appThreeInputTopic, Consumed.with(stringSerde, loginEventSerde));

        final Aggregator<String, UserEvent, UserRollup> loginAggregator = new LoginAggregator();

        final KGroupedStream<String, UserEvent> appOneGrouped = appOneStream.groupByKey();
        final KGroupedStream<String, UserEvent> appTwoGrouped = appTwoStream.groupByKey();
        final KGroupedStream<String, UserEvent> appThreeGrouped = appThreeStream.groupByKey();

        appOneGrouped.cogroup(loginAggregator)
            .cogroup(appTwoGrouped, loginAggregator)
            .cogroup(appThreeGrouped, loginAggregator)
            
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(1)))
            
            .aggregate(() -> new UserRollup(new HashMap<>()), Materialized.with(Serdes.String(), loginRollupSerde))
            .toStream()
            .filter((k, v) -> v != null)
            .to(totalResultOutputTopic, Produced
            .with(new TimeWindowedSerde<>(Serdes.String(), Long.MAX_VALUE), loginRollupSerde));

        return builder.build();
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Properties allProps) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure((Map)allProps, false);
        return specificAvroSerde;
    }

    public void createTopics(final Properties allProps) {
        try (final AdminClient client = AdminClient.create(allProps)) {

            final List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                allProps.getProperty("app-one.topic.name"),
                Integer.parseInt(allProps.getProperty("app-one.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-one.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("app-two.topic.name"),
                Integer.parseInt(allProps.getProperty("app-two.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-two.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("app-three.topic.name"),
                Integer.parseInt(allProps.getProperty("app-three.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-three.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("output.topic.name"),
                Integer.parseInt(allProps.getProperty("output.topic.partitions")),
                Short.parseShort(allProps.getProperty("output.topic.replication.factor"))));

            client.createTopics(topics);
        }
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        final Properties allProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        final CogroupingStreams instance = new CogroupingStreams();
        final Properties allProps = instance.loadEnvProperties(args[0]);
        final Topology topology = instance.buildTopology(allProps);

        instance.createTopics(allProps);

        TutorialDataGenerator dataGenerator = new TutorialDataGenerator(allProps);
        dataGenerator.generate();

        allProps.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimestampExtractor.class.getName());
        // Setting this to a low value on purpose to flush the cache quickly during the demo
        // Normally you'll want to keep this setting at the default value of 30 seconds
        allProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        
        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    static class TutorialDataGenerator {
        final Properties properties;


        public TutorialDataGenerator(final Properties properties) {
            this.properties = properties;
        }

        public void generate() {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            
           
            Instant instant = Instant.now();
            Long time = instant.toEpochMilli();

            try (Producer<String, UserEvent> producer = new KafkaProducer<String, UserEvent>(properties)) {
                HashMap<String, List<UserEvent>> entryData = new HashMap<>();

                List<UserEvent> messages1 = Arrays.asList(new UserEvent("one", "Ted", time),
                    new UserEvent("one", "Ted", time),
                    new UserEvent("one", "Carol", time),
                    new UserEvent("one", "Carol", time),
                    new UserEvent("one", "Alice", time),
                    new UserEvent("one", "Carol", time));
                final String topic1 = properties.getProperty("app-one.topic.name");
                entryData.put(topic1, messages1);

                List<UserEvent> messages2 = Arrays.asList(new UserEvent("two", "Bob", time),
                    new UserEvent("two", "Carol", time),
                    new UserEvent("two", "Ted", time),
                    new UserEvent("two", "Carol", time));
                final String topic2 = properties.getProperty("app-two.topic.name");
                entryData.put(topic2, messages2);

                List<UserEvent> messages3 = Arrays.asList(new UserEvent("three", "Bob", time),
                    new UserEvent("three", "Alice", time),
                    new UserEvent("three", "Alice", time),
                    new UserEvent("three", "Carol", time));
                final String topic3 = properties.getProperty("app-three.topic.name");
                entryData.put(topic3, messages3);


                entryData.forEach((topic, list) ->
                    list.forEach(message ->
                        producer.send(new ProducerRecord<String, UserEvent>(topic, message.getUserId(), message), (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace(System.out);
                            } else {
                                System.out.printf("Produced record at offset %d to topic %s %n", metadata.offset(), metadata.topic());
                            }
                        })
                    )
                );
            }
        }
    }

}
