package com.github.zjiajun.kafka.consumer

import com.github.zjiajun.kafka.config.Config
import kafka.api.{OffsetFetchRequest, OffsetRequest, PartitionOffsetRequestInfo, Request}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer


/**
  * @author zhujiajun
  * @since 2016/11/18
  */
object TimeConsumer extends App {

  val standaloneBrokerList: String = Config.standaloneBrokerList()
  val hostAndPort = standaloneBrokerList.split(":")
  val topic = "compaction-test"
  val clientId = "timeClient"

  val simpleConsumer = new SimpleConsumer(hostAndPort(0),hostAndPort(1).toInt,10000,4096,clientId)

  val brokerList = ClientUtils.parseBrokerList(standaloneBrokerList)
  val topicMetadataResponse = ClientUtils.fetchTopicMetadata(Set(topic),brokerList,clientId,10000)

  val offsetRequest = OffsetRequest(requestInfo = Map(TopicAndPartition(topic,0) -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)),
    clientId = clientId, replicaId = Request.OrdinaryConsumerId)

  val offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest)

  val partitionErrorAndOffsets = offsetResponse.partitionErrorAndOffsets(TopicAndPartition(topic,0))

  val offset = partitionErrorAndOffsets.error match {
    case 0 => partitionErrorAndOffsets.offsets
    case _ => throw new RuntimeException()
  }

  println(offset)

}
