package com.github.zjiajun.kafka.config

import java.util.Properties

/**
  * @author zhujiajun
  * @since 2016/11/20
  */
object kafkaProperties {

  def consumerProp(zkString: String, groupId: String): Properties =  {
    val prop = new Properties()
    prop.put("zookeeper.connect",zkString)
    prop.put("group.id",groupId)
    prop.put("zookeeper.session.timeout.ms", "10000")
    prop.put("zookeeper.sync.time.ms", "10000")
    prop.put("auto.offset.reset", "smallest") //smallest
    prop.put("auto.commit.enable", "true")
    prop.put("auto.commit.interval.ms", "5000")
    prop
  }

}
