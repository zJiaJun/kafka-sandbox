package com.github.zjiajun.kafka.producer;

import com.github.zjiajun.kafka.config.Config;
import com.github.zjiajun.kafka.partitioner.RoundRobinPartitioner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by zhujiajun
 * 16/3/23 21:33
 *
 * kafka 082 版本 生产demo
 */
public class Producer082 {

    public static void main(String[] args) {
        long watch = System.currentTimeMillis();
        Properties properties = new Properties();
        properties.put("metadata.broker.list", Config.standaloneBrokerList());
        properties.put("producer.type","sync");
        properties.put("serializer.class", StringEncoder.class.getCanonicalName());
        properties.put("key.serializer.class", StringEncoder.class.getCanonicalName());
        properties.put("partitioner.class", RoundRobinPartitioner.class.getCanonicalName());
        properties.put("request.required.acks","-1");

        //Async use
        properties.put("queue.buffering.max.ms","5000");
        properties.put("queue.buffering.max.messages","10000");
        properties.put("queue.enqueue.timeout.ms","-1");
        properties.put("batch.num.messages","200");


        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String,String> producer = new Producer<>(producerConfig);

        List<KeyedMessage<String,String>> keyedMessages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3;j++) {
                KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("test", String.valueOf(i), i + "_message_" + j);
                keyedMessages.add(keyedMessage);
            }
        }

//        KeyedMessage<String,String> msg = new KeyedMessage<>("test-topic","key","cluster kafka");
        producer.send(keyedMessages);
        System.out.println(System.currentTimeMillis() - watch + " :ms");
        producer.close();
    }

}
