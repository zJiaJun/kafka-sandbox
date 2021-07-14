package com.github.zjiajun.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author zhujiajun
 * @version 1.0
 * @since 2021/7/10 11:27
 */
public class ProducerExample28 {

    public static void main(String[] args) throws InterruptedException {
        String topic ="test";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record_1 = new ProducerRecord<>(topic, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()));
        Headers headers = record_1.headers();
        headers.add("delaySeconds", "120".getBytes());

        producer.send(record_1, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("onCompletion");
            }
        });



        producer.close();
    }
}
