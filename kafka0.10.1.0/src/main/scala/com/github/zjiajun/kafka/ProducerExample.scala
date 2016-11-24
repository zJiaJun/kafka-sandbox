package com.github.zjiajun.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.serialization.StringSerializer

/**
  * @author zhujiajun
  * @since 2016/11/24
  */
object ProducerExample {

  val topic = "test"
  val props = new Properties()
  props.put("bootstrap.servers","127.0.0.1:9092")
  props.put("acks","all")
  props.put("retries", 3)
  props.put("batch.size", 16384)
  props.put("linger.ms", 1)
  props.put("buffer.memory", 33554432)
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  props.put("partitioner.class", classOf[DefaultPartitioner].getName)

  val kafkaProducer = new KafkaProducer[String,String](props)
  val record = new ProducerRecord[String,String](topic,"key","value")
  for (i <- 0 to 10) {
    kafkaProducer.send(record,new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) exception.printStackTrace()
        if (metadata != null) {
          println("Send record partition:%d, offset:%d, keysize:%d, valuesize:%d %n",
            metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
            metadata.serializedValueSize())
        }

      }
    })
  }
  kafkaProducer.close()

}
