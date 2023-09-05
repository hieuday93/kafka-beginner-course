package io.konduktor.demos.kafa.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.StreamException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    public static void main(String[] args) {
        log.info("I'm a producer!");

        String bootstrapServers = "localhost:9092";
        String topic = "wikimedia.recentchanges";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // set safe producer configs (Kafka <= 2.8)
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // set producer compression config for higher throughput (in expense for latency)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String eventUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(URI.create(eventUrl));
        EventSource eventSource = builder.build();

        try {
            LocalDateTime start = LocalDateTime.now();
            eventSource.start();
            while (true) {
                MessageEvent messageEvent = eventSource.readMessage();
                if (messageEvent != null) {
                    log.info("sending message: {}", messageEvent.getData());
                    producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
                }
                Duration duration = Duration.between(start, LocalDateTime.now());
                if(duration.compareTo(Duration.ofSeconds(5)) > 0) {
                    log.info("reach for 10 seconds, stop reading new events");
                    break;
                }
            }
        } catch (StreamException e) {
            throw new RuntimeException(e);
        } finally {
            eventSource.close();
            producer.close();
            log.info("Producer closed");
        }

    }

}
