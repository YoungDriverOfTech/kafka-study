package com.liang.kafkapricatice.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartition implements Partitioner {

    /*
    * 重写分区器，规定什么样的数据进入那个分区
    * */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*
        * 假如数据的key像这样,并且有两个分区0和1
        * key-1
        * key-2
        * key-3
        * */
        String keyStr = key + "";
        String keyInt = keyStr.substring(3);

        // 使用除2取余数来决定消息进入哪个partition
        int i = Integer.parseInt(keyInt);
        return i % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
