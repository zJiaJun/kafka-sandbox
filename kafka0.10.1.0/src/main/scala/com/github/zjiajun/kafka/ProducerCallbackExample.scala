package com.github.zjiajun.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.serialization.StringSerializer

/**
  * @author zhujiajun
  * @since 2016/11/24
  */
object ProducerCallbackExample {

  def main(args: Array[String]): Unit = {
    val topic = "test"
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
    props.put(ACKS_CONFIG,"all")
    props.put(RETRIES_CONFIG, "3")
    props.put(BATCH_SIZE_CONFIG, "16384")
    props.put(LINGER_MS_CONFIG, "1")
    props.put(BUFFER_MEMORY_CONFIG, "33554432")
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(PARTITIONER_CLASS_CONFIG, classOf[DefaultPartitioner].getName)

    val kafkaProducer = new KafkaProducer[String,String](props)
    for (i <- 0 to 10) {
      val record = new ProducerRecord[String,String](topic, s"whynot3key_$i", s"value_$i")
      kafkaProducer.send(record,new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) exception.printStackTrace()
          if (metadata != null) {
            println(s"Send record partition:${metadata.partition()}," +
              s"offset:${metadata.offset()}, " +
              s"keysize:${metadata.serializedKeySize()}, valuesize:${metadata.serializedValueSize()}")
          }

        }
      })
    }
    kafkaProducer.close()
  }


}
