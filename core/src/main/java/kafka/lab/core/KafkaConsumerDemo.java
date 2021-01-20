package kafka.lab.core;

import kafka.lab.KafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static kafka.lab.KafkaProperties.KAFKA_CLUSTER_SERVER_URL;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class KafkaConsumerDemo {

    public void consume(boolean async) {
        String groupId = "123";
        int messageRemaining =100;
        Properties kafkaPropertie = new Properties();
        kafkaPropertie.put("bootstrap.servers", KAFKA_CLUSTER_SERVER_URL);
        kafkaPropertie.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaPropertie.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaPropertie.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("consumer-%s", System.currentTimeMillis()));
        kafkaPropertie.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPropertie.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        kafkaPropertie.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");

        Consumer consumer = new KafkaConsumer<String, String>(kafkaPropertie);
        consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord record : records) {
            System.out.println(groupId + " received message : from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
        messageRemaining -= records.count();
        consumer.commitAsync();
        if (messageRemaining <= 0) {
        }
    }
}
