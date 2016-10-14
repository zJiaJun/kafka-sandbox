package com.github.zjiajun.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * @author zhujiajun
 * @since 2016/10/13
 *
 * 随机分配
 */
public class RandomPartitioner implements Partitioner {

    public RandomPartitioner(VerifiableProperties verifiableProperties) {
        System.err.println("构造函数: " + this);
        /*
         根据配置的partitioner.class来构建Partitioner对象，源代码如下
         Utils.createObject[Partitioner](config.partitionerClass, config.props)
         config.props就是VerifiableProperties的实例，所以构造函数需要此参数
         */
    }

    private final Random random = new Random();

    @Override
    public int partition(Object key, int numPartitions) {
        System.err.println("partition method: " + this);
        return random.nextInt(numPartitions);
    }

}
