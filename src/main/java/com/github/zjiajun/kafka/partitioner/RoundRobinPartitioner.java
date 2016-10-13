package com.github.zjiajun.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhujiajun
 * @since 2016/10/13
 *
 * 轮询分配
 */
public class RoundRobinPartitioner implements Partitioner {

    private static AtomicLong next = new AtomicLong();

    public RoundRobinPartitioner(VerifiableProperties verifiableProperties) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        long nextIndex = next.incrementAndGet();
        return (int) nextIndex % numPartitions;
    }
}
