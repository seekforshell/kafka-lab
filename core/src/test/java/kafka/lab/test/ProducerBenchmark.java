package kafka.lab.test;

import kafka.lab.core.Producer;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class ProducerBenchmark {
    @Test
    public void Producer1() {
        String topicName1 = "ticktok_live";
        String topicName2 = "ticktok_reword";
        int msgCnt = 100;
        CountDownLatch latch1 = new CountDownLatch(1);
        int transactionTimeoutMs = 0;
        Producer p1 = new Producer(topicName1, false, null, true, msgCnt, transactionTimeoutMs, latch1);
        p1.run();

        CountDownLatch latch2 = new CountDownLatch(1);
        Producer p2 = new Producer(topicName2, false, null, true, msgCnt, transactionTimeoutMs, latch2);
        p2.run();
    }
}
