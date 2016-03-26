package com.github.zjiajun.kafka.producer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
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

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        long watch = System.currentTimeMillis();
        Properties properties = new Properties();
        properties.put("metadata.broker.list","127.0.0.1:9092");
        properties.put("serializer.class", DefaultEncoder.class.getCanonicalName());
        properties.put("key.serializer.class", StringEncoder.class.getCanonicalName());

        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer producer = new kafka.javaapi.producer.Producer(producerConfig);
        List<KeyedMessage<String,byte[]>> keyedMessages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte [] content =  ("kafka082_value_" + i).getBytes();
            KeyedMessage<String,byte[]> keyedMessage =  new KeyedMessage<>("topic_082","kafka082_key_" + i,content);
            keyedMessages.add(keyedMessage);
        }
        producer.send(keyedMessages);
        System.out.println(System.currentTimeMillis() - watch + " :ms");
        producer.close();
    }

}
