package kafka.lab.test;

import kafka.lab.core.AdminClientHelper;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class KafkaClientTest {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void testClient() throws ExecutionException, InterruptedException {
        AdminClient client = AdminClientHelper.newClient();
        logger.info("cluster info:{}", client.describeCluster().clusterId().get().toString());

    }
}
