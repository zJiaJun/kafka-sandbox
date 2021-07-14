package com.github.zjiajun.kafka;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhujiajun
 * @version 1.0
 * @since 2021/7/10 12:34
 */
public class TTLCustomInterceptor implements ConsumerInterceptor<String,String> {


    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            if (!"test".equals(partition.topic())) {
                continue;
            }
            List<ConsumerRecord<String, String>> recordList = records.records(partition);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : recordList) {
                Headers headers = record.headers();
                long delaySeconds = -1;
                for (Header header : headers) {
                    if (header.key().equals("delaySeconds")) {
                        delaySeconds = Long.parseLong(new String(header.value()));
                        if (delaySeconds > 0 && now - record.timestamp() >= delaySeconds * 1000) {
                            newTpRecords.add(record);
                        }
                    }
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(partition, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((t,o) -> System.err.println("tp:" + t +", offset:" + o));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
