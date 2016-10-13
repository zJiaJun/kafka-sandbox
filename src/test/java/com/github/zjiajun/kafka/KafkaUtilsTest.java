package com.github.zjiajun.kafka;

import com.github.zjiajun.kafka.partitioner.RandomPartitioner;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * @author zhujiajun
 * @since 2016/10/13
 */
public class KafkaUtilsTest {

    @Test
    public void createObject() {
        try {
            Class<Partitioner> aClass = (Class<Partitioner>) Class.forName(RandomPartitioner.class.getCanonicalName());
            Constructor<Partitioner> constructor = aClass.getConstructor(new Class[]{VerifiableProperties.class});
            Partitioner partitioner = constructor.newInstance(new Object[]{new VerifiableProperties(new Properties())});
            System.out.println(partitioner);
            int partition = partitioner.partition(0, 3);
            System.out.println(partition);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
