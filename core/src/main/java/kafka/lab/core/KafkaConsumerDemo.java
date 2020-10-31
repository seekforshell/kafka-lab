package kafka.lab.core;

import kafka.lab.KafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class KafkaConsumerDemo {

    public void consume(boolean async) {
        String groupId = "123";
        int messageRemaining =100;
        Properties kafkaPropertie = new Properties();
        kafkaPropertie.put("bootstrap.servers", "192.168.90.71:9092");
        kafkaPropertie.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPropertie.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaPropertie.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaPropertie.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer consumer = new KafkaConsumer<String, String>(kafkaPropertie);
        consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord record : records) {
            System.out.println(groupId + " received message : from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
        messageRemaining -= records.count();
        if (messageRemaining <= 0) {
        }
    }
}
