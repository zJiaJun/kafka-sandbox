package com.github.zjiajun.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhujiajun
 * 16/3/24 21:15
 *
 * kafka 082 版本 消费demo
 */
@SuppressWarnings("unchecked")
public class Consumer082 {

    private ConsumerConnector consumerConnector;
    private String topic;
    private ExecutorService executorService;


    private ConsumerConfig createConsumerConfig(String zkConnect,String groupId) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect",zkConnect);
        properties.put("group.id",groupId);
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(properties);
    }

    public Consumer082(String zkConnect, String groupId, String topic) {
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zkConnect,groupId));
        this.topic = topic;
    }

    public void shutdown() {
        if (consumerConnector != null) consumerConnector.shutdown();
        if (executorService != null) executorService.shutdown();
    }

    public void run(int threadNum) {
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, threadNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);

        executorService = Executors.newFixedThreadPool(threadNum);

        int threadNumber = 0;
        for (KafkaStream stream : kafkaStreams) {
            executorService.submit(new ConsumerThread(stream,threadNumber));
            threadNumber++;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String zkConnect = "127.0.0.1:2181/kafka";
        String groupId = "kafka_consumer_demo";
        String topic = "topic_082";
        Consumer082 consumer082 = new Consumer082(zkConnect,groupId,topic);
        consumer082.run(5);

        TimeUnit.SECONDS.sleep(3);
        consumer082.shutdown();
    }

    private class ConsumerThread implements Runnable {

        private KafkaStream<byte[],byte[]> kafkaStream;
        private int threadNum;

        public ConsumerThread(KafkaStream<byte[], byte[]> kafkaStream, int threadNum) {
            this.kafkaStream = kafkaStream;
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            System.out.println("begin consumer....");
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                System.out.println("------");
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                byte[] message = next.message();
                System.out.println("Thread " + threadNum + ":" + new String(message));
            }
            System.out.println("Shutdown thread " + threadNum );
        }
    }
}
