package kafka.lab.core;

import kafka.lab.KafkaProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.util.Properties;

import static org.apache.kafka.clients.admin.AdminClientConfig.RETRY_BACKOFF_MS_CONFIG;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
public class AdminClientHelper {
    private volatile static AdminClient client;
    public static AdminClient newClient() {
        if (null == client) {
            synchronized (AdminClientHelper.class) {
                Properties confs = new Properties();
                confs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_CLUSTER_SERVER_URL);
                confs.put(RETRY_BACKOFF_MS_CONFIG, 5000);
                client = AdminClient.create(confs);
            }
        } else {
            return client;
        }

        return client;

    }
}
