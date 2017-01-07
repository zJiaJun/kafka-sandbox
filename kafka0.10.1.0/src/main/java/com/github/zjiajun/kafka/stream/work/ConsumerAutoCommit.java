package com.github.zjiajun.kafka.stream.work;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhujiajun
 * @since 2017/1/7
 */
public class ConsumerAutoCommit {

    public static void main(String[] args) {
        String bootstrap = "127.0.0.1:9092";
        String topic = "gender-amount";
        String groupid = "final-group";
        String clientid = "final-client";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupid);
        props.put("client.id", clientid);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n", clientid, record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());
            });
        }
    }
}
