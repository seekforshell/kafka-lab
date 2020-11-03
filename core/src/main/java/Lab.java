import kafka.lab.core.KafkaConsumerDemo;
import kafka.lab.core.KafkaProducerDemo;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class Lab {
    public static void main(String[] args) {
//        KafkaProducerDemo producerDemo = new KafkaProducerDemo();
//        producerDemo.send();

        KafkaConsumerDemo consumerDemo = new KafkaConsumerDemo();
        consumerDemo.consume(true);
    }
}
