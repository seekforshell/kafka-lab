import kafka.lab.core.KafkaConsumerDemo;
import kafka.lab.core.KafkaProducerDemo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import static kafka.lab.KafkaProperties.KAFKA_CLUSTER_SERVER_URL;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class Lab {
    public static void main(String[] args) {
        Properties kafkaPropertie = new Properties();
        KafkaProducerDemo producerDemo = new KafkaProducerDemo();

        kafkaPropertie.put("bootstrap.servers", KAFKA_CLUSTER_SERVER_URL);

//        KafkaConsumerDemo consumerDemo = new KafkaConsumerDemo();
//        consumerDemo.consume(true);
    }
}
