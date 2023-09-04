package io.konduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("I'm a producer!");

        //region create Producer properties
        Properties properties = new Properties();

        //region connect to Konduktor platform
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6ayFReQAmRG1HpGow8Dt6P\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2YXlGUmVRQW1SRzFIcEdvdzhEdDZQIiwib3JnYW5pemF0aW9uSWQiOjc2MDkwLCJ1c2VySWQiOjg4NTQ1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhZmU5ZmJlMi01ZDk2LTQ5MmMtYjhmMS1lOTNmMDE2NzcwZjUifX0.UeLGGqPmspRJhjRHZC9opoPVeogG0KMw6He4FT4uyCo\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        //endregion

        //region connect to local Kafka cluster
//        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        //endregion

        //region producer settings
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //should not do these in production
        properties.setProperty("batch.size", "400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //endregion

        //endregion

        //region create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //endregion

        //region send message
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                producer.send(
                        new ProducerRecord<>("demo_java", "hello world " + i),
                        (metadata, exception) -> {
                            if (exception == null) {
                                log.info("Received new metadata. \n" +
                                        "Topic: " + metadata.topic() + "\n" +
                                        "Partition: " + metadata.partition() + "\n" +
                                        "Offset: " + metadata.offset() + "\n" +
                                        "Timestamp: " + metadata.timestamp());
                            } else {
                                log.error("Error while producing", exception);
                            }
                        });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Error while producing", e);
                Thread.currentThread().interrupt();
            }
        }
        //endregion

        //region flush and close the producer
        //synchronously send all remaining messages and block until all messages are sent
        producer.flush();

        //implicitly flush and close the producer
        producer.close();
        //endregion

    }

}
