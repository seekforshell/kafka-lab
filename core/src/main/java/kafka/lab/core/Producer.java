/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.lab.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.lab.KafkaProperties;
import kafka.lab.core.model.TickTokLive;
import kafka.lab.core.model.TickTokReword;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Producer extends Thread {

    private static List<String> fansIds = new ArrayList<>();
    private static List<String> liveIds = new ArrayList<>();
    private static List<String> giftIds = new ArrayList<>();
    private static Random random = new Random();
    static {
        fansIds.add("fans_001");
        fansIds.add("fans_002");
        fansIds.add("fans_003");

        giftIds.add("helicopter");
        giftIds.add("unicorn");
        giftIds.add("boom");
    }

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private int numRecords;
    private final CountDownLatch latch;
    private final String liveId = "live001";

    public Producer(final String topic,
                    final Boolean isAsync,
                    final String transactionalId,
                    final boolean enableIdempotency,
                    final int numRecords,
                    final int transactionTimeoutMs,
                    final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ticktokLive_demo");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);

        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
    }

    KafkaProducer<Integer, String> get() {
        return producer;
    }

    @Override
    public void run() {
        int messageKey = 0;
        int recordsSent = 0;
        while (recordsSent < numRecords) {
            String messageStr = "";
            DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String current_time = LocalDateTime.now().format(DATETIME_FORMATTER);
            if (KafkaConst.TOPIC_LIVE.equals(this.topic)) {
                TickTokLive live = new TickTokLive();
                live.setLive_time(current_time);
                live.setBlogger_id("1001");
                live.setLive_id(liveId);
                liveIds.add(liveId);
                ObjectMapper msg = new ObjectMapper();
                try {
                    messageStr = msg.writeValueAsString(live);
                } catch (JsonProcessingException e) {
                    log.error("json convert error", e);
                }
            } else if (KafkaConst.TOPIC_REWORD.equals(this.topic)) {
                TickTokReword reword = new TickTokReword();
                // 随机生成粉丝Id
                reword.setFans_id(fansIds.get(random.nextInt(fansIds.size())));
//                reword.setLive_id(liveIds.get(random.nextInt(liveIds.size())));
                reword.setLive_id(liveId);
                reword.setReward_gift_num(random.nextInt(10));
                reword.setReward_time(current_time);
                reword.setReward_gift_id(giftIds.get(random.nextInt(giftIds.size())));
                ObjectMapper msg = new ObjectMapper();
                try {
                    messageStr = msg.writeValueAsString(reword);
                } catch (JsonProcessingException e) {
                    log.error("json convert error", e);
                }
            }


            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                    messageKey,
                    messageStr), null);
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic, messageStr)).get();
                    System.out.println("Sent message: (" + messageKey + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            recordsSent += 1;
        }
        System.out.println("Producer sent " + numRecords + " records successfully");
        latch.countDown();
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
     * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
