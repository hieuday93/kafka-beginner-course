package io.konduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        log.info("I'm a consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

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
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
//        properties.setProperty("group.instance.id", "... for static consumer");
        //endregion

        //endregion

        //region create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //endregion

        //region subscribe to topic
        consumer.subscribe(List.of(topic));
        //endregion

        //region add shutdown hook
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Error: ", e);
                Thread.currentThread().interrupt();
            }
        }));
        //endregion

        //region poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    log.info("Key: {}, Value: {}\nPartition: {}, Offset: {}",
                            consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal");
        } catch (Exception e) {
            log.error("Error: ", e);
        } finally {
            consumer.close();
            log.info("Gracefully shutdown the consumer");
        }
        //endregion

    }

}
