package com.github.zjiajun.kafka.consumer

import com.github.zjiajun.kafka.config.kafkaProperties
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

import scala.collection.Map

/**
  * @author zhujiajun
  * @since 2016/11/20
  *
  * 一条消息只会被一个组下的任意消费者消费，单播
  *
  * 一条消息会被不同组下的消费者消息，多播
  */
object GroupConsumer extends App {


  val topic =  args(0)
  val consumerProp = kafkaProperties.consumerProp("127.0.0.1:2181/kafka0.8.2.2",args(1))

  val consumerConfig = new ConsumerConfig(consumerProp)

  val consumerConnector = Consumer.create(consumerConfig)

  val map = scala.collection.immutable.Map[String,Int](topic -> 1)

  val streams: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumerConnector.createMessageStreams(map)

  streams(topic).head.foreach {v =>
    println("offset : " +  v.offset)
    println("partition : " + v.partition)
//    println("key : " + new String(v.key()))
    println("value : " + new String(v.message()))
  }


}
