// Kafka receiver example from Microsoft Fabric Custom Destination

package EventStream.Sample;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaReceiver {
    private static final String connectionString = "Endpoint=sb://";
    private static final String eventHubName = "";
    private static final String consumerGroupName = "";

    public static void main(String[] args) {
        // Initialize the Properties
        Properties props = getProperties();

        // Create the Consumer and receive the message
        try (Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe to the topic (eventHubName)
            consumer.subscribe(Collections.singletonList(eventHubName));
            consumeEvents(consumer);
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        String namespace = connectionString.substring(connectionString.indexOf("/") + 2, connectionString.indexOf("."));
        props.put("bootstrap.servers", String.format("%s.servicebus.windows.net:9093", namespace));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("group.id", consumerGroupName);
        props.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="%s";", connectionString));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static void consumeEvents(Consumer<Long, String> consumer) {
        // Set the consumer offset to be zero, as you may have already published events to the event hub.
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<Long, String> cr : consumerRecords) {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)
", cr.key(), cr.value(), cr.partition(), cr.offset());
            }
            consumer.commitAsync();
        }
    }
}