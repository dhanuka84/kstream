# kstream

./gradlew build

./gradlew shadowJar

docker-compose -f docker-compose.yml up -d

docker exec -it schema-registry /usr/bin/kafka-avro-console-consumer --topic event-output-topic --bootstrap-server broker:9092 --from-beginning


{"event_by_user":{"Bob":{"three":1,"two":1}}}
{"event_by_user":{"Ted":{"two":1,"one":2}}}
{"event_by_user":{"Alice":{"three":2,"one":1}}}
{"event_by_user":{"Carol":{"three":1,"two":2,"one":3}}}


#clean up

rm -rf /tmp/kafka-streams/
rm -rf /tmp/*
