package kafka.lab.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.lab.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static kafka.lab.KafkaProperties.KAFKA_CLUSTER_SERVER_URL;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
@Slf4j
public class KafkaProducerDemo {


    public void send(Properties kafkaPropertie, String topicName, String msg) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaPropertie);
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaProperties.TOPIC, topicName,
                msg);

        try {
            Future<RecordMetadata> future = kafkaProducer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.toString());
        } catch (InterruptedException e) {
            log.error("send is interrupted", e);
        } catch (ExecutionException e) {
            log.error("execute exception", e);
        }
    }
}
