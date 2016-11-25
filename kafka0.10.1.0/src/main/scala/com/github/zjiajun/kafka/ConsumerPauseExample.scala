package com.github.zjiajun.kafka

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author zhujiajun
  * @since 2016/11/25
  */
object ConsumerPauseExample {

  def main(args: Array[String]): Unit = {
    val topic = "test"
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,"test.client")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"3000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer].getName)

    val kafkaConsumer = new KafkaConsumer[String,String](props)
    kafkaConsumer.subscribe(Collections.singletonList(topic))
    while (true) {
      val consumerRecords: ConsumerRecords[String, String] = kafkaConsumer.poll(100)
      kafkaConsumer.pause(util.Arrays.asList(new TopicPartition(topic,0),new TopicPartition(topic,1)))

      val iter = consumerRecords.iterator()
      while (iter.hasNext) {
        val record: ConsumerRecord[String, String] = iter.next()
        println(record)
      }
    }
  }

}
