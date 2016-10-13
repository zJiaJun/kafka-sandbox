package com.github.zjiajun.kafka.consumer;

/**
 * Created by zhujiajun
 * 16/3/25 22:05
 *
 * kafka 090 版本 消费demo
 */
public class Consumer090 {

    /*
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "consumer_090");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "10000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("topic_090"));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println("-------------------");
            }
        }
    }
    */
}
