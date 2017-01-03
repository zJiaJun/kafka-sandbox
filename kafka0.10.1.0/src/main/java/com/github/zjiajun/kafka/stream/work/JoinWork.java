package com.github.zjiajun.kafka.stream.work;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zhujiajun
 * @since 2016/12/2
 */
public class JoinWork {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-work");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181/kafka0.10.1.0");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String, String> kStream = kStreamBuilder.stream(Serdes.String(), Serdes.String(), "stream-join");
        KTable<String, String> kTable = kStreamBuilder.table(Serdes.String(), Serdes.String(), "stream-table", "table-state-store");

        KStream<String, String> stringObjectKStream = kStream.leftJoin(kTable, (s1, s2) -> s1 + "***" + s2);
        stringObjectKStream.foreach((key,value) -> System.out.println("foreach : " + key +"_____" + value));

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }
}
