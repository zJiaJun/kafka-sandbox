package com.github.zjiajun.kafka;

import com.github.zjiajun.kafka.config.Config;
import com.github.zjiajun.kafka.partitioner.RandomPartitioner;
import kafka.api.TopicMetadata;
import kafka.producer.BrokerPartitionInfo;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.serializer.StringEncoder;
import scala.collection.mutable.HashMap;
import scala.collection.immutable.HashSet;
import scala.collection.mutable.StringBuilder;

import java.util.Properties;

/**
 * @author zhujiajun
 * @since 2016/10/24
 */
public class FetchTopicMetadata {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", Config.standaloneBrokerList());
        properties.put("producer.type","sync");
        properties.put("serializer.class", StringEncoder.class.getCanonicalName());
        properties.put("key.serializer.class", StringEncoder.class.getCanonicalName());
        properties.put("partitioner.class", RandomPartitioner.class.getCanonicalName());
        properties.put("request.required.acks","-1");

        //Async use
        properties.put("queue.buffering.max.ms","5000");
        properties.put("queue.buffering.max.messages","10000");
        properties.put("queue.enqueue.timeout.ms","-1");
        properties.put("batch.num.messages","200");


        ProducerConfig producerConfig = new ProducerConfig(properties);
        ProducerPool producerPool = new ProducerPool(producerConfig);
        HashMap<String,TopicMetadata> metadataHashMap = new HashMap<>();
        BrokerPartitionInfo brokerPartitionInfo = new BrokerPartitionInfo(producerConfig,producerPool,metadataHashMap);
        scala.collection.immutable.Set<String> set = new HashSet<>();
        set.addString(new StringBuilder("topic01"));
        brokerPartitionInfo.updateInfo(set,1);

    }
}
