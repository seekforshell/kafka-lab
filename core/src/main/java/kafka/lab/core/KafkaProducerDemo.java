package kafka.lab.core;

import kafka.lab.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class KafkaProducerDemo {


    public void send() {
        Properties kafkaPropertie = new Properties();
        //配置broker地址，配置多个容错
        kafkaPropertie.put("bootstrap.servers", "192.168.90.71:9092");
        //配置key-value允许使用参数化类型
        kafkaPropertie.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaPropertie.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaPropertie);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(KafkaProperties.TOPIC,"kafkaLab","hello world");

        try {
            Future<RecordMetadata> future = kafkaProducer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
