package com.github.zjiajun.kafka

import java.util.concurrent.Executors
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author zhujiajun
  * @since 2016/11/25
  *
  * 错误例子
  *
  * KafkaConsumer 线程不安全，多线程执行会抛异常
  * java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
  */
object MultiConsumerExample {

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

    val pool =  Executors.newFixedThreadPool(3)
    for (i <- 1 to 3) {
      pool.execute(new Runnable {
        override def run(): Unit = {
          println(Thread.currentThread().getId)
          kafkaConsumer.subscribe(Collections.singletonList(topic))
          while (true) {
            val consumerRecords: ConsumerRecords[String, String] = kafkaConsumer.poll(100)
            val iter = consumerRecords.iterator()
            while (iter.hasNext) {
              val record: ConsumerRecord[String, String] = iter.next()
              println(record)
            }
          }
        }
      })
    }
  }
}
