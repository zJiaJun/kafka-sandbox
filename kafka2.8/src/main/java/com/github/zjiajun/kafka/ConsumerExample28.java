package com.github.zjiajun.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author zhujiajun
 * @version 1.0
 * @since 2021/7/10 11:34
 */
public class ConsumerExample28 {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group.example");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, TTLCustomInterceptor.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(3));
            assignment = consumer.assignment();
        }
        while (true) {
            System.err.println("nowTime:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()));
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition topicPartition : assignment) {
                timestampsToSearch.put(topicPartition, System.currentTimeMillis() - 180 * 1000);
            }
            Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(timestampsToSearch);

            for (TopicPartition topicPartition : assignment) {
                OffsetAndTimestamp offsetAndTimestamp = offsetAndTimestampMap.get(topicPartition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(topicPartition, offsetAndTimestamp.offset());
                }
            }
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.err.println(record.toString());
            }
            consumer.commitSync();
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
