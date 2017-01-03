package com.github.zjiajun.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Arrays;
import java.util.Properties;

public class WordCountDSL {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-dsl");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181/kafka0.10.1.0");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> stream = builder.stream("words");
//		stream.flatMapValues(values -> Arrays.asList(values.toLowerCase().split(" ")))
//				.map((k, v) -> KeyValue.<String, String>pair(v, v)).groupByKey().aggregate(
//						() -> 0L,
//						(aggKey, value, aggregate) -> aggregate + 1L, 
//						TimeWindows.of(5000).advanceBy(1000),
//						Serdes.Long(), 
//						"Counts1")
//				.foreach((Windowed<String> window, Long value) -> {
//					System.out.printf("key=%s, value=%s, start=%d, end=%d\n",window.key(), value, window.window().start(), window.window().end());
//				});
		
		KStream<String, String> kStream = stream.flatMapValues(values -> Arrays.asList(values.toLowerCase().split(" ")))
		.map((k, v) -> KeyValue.<String, String>pair(v, v)).groupByKey().aggregate(
				() -> 0L,
				(aggKey, value, aggregate) -> aggregate + 1L, 
				TimeWindows.of(5000).advanceBy(5000),
				Serdes.Long(),
				"Counts")
		.toStream()
		.map((Windowed<String> window, Long value) -> {
			return new KeyValue<String, String>(window.key(), String.format("key=%s, value=%s, start=%d, end=%d\n",window.key(), value, window.window().start(), window.window().end()));
			});
		kStream.to(Serdes.String(), Serdes.String(), "count");
		
//		KTable<String, Long> kTable = stream.flatMapValues(values -> Arrays.asList(values.toLowerCase().split(" ")))
//				.map((k, v) -> KeyValue.<String, String>pair(v, v)).groupByKey().count("Counts");
//		kTable.to(Serdes.String(), Serdes.Long(), "stream-dsl-sink");

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
		Thread.sleep(100000L);
		streams.close();
	}

}
