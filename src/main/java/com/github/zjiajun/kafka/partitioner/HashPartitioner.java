package com.github.zjiajun.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author zhujiajun
 * @since 2016/10/13
 *
 * Hash分配
 */
public class HashPartitioner implements Partitioner {

    public HashPartitioner(VerifiableProperties verifiableProperties) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        if ((key instanceof Integer)) {
            return Math.abs(Integer.parseInt(key.toString())) % numPartitions;
        }
        return Math.abs(key.hashCode() % numPartitions);
    }
}
