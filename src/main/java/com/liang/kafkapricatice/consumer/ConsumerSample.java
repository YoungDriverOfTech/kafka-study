package com.liang.kafkapricatice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerSample {

    private static final String TOPIC_NAME = "first-topic";

    public static void main(String[] args) {

        // 1. 最简单的消费者
        helloWorld();

        // 2. 手动提交
        commitOffsetManually();

        // 3. 手动提交offset,并且手动控制partition
        commitOffWithPartition();

        // 4. 手动订阅某个分区
        commitOffWithPartition2();

        // 5. 手动控制offset

        // 6. 流量控制
        controlPause();
    }

    /*
    * 最简单的消费者（工作中不推荐）
    * */
    private static void helloWorld() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅哪个topic
        consumer.subscribe(List.of(TOPIC_NAME));
        while (true) {
            // 没间隔100毫秒去kafka拉取记录
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    /*
    * 手动提交offset
    * */
    private static void commitOffsetManually() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false"); // 关闭自动提交
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅哪个topic
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            // 没间隔100毫秒去kafka拉取记录
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // 把数据存入数据库
                // 假设这块已经存入了db

                // 如果失败则回滚，不要提交offset
            }

            // 手动通知offSet提交，如果没提交，则消息还是存在kafka中。
            consumer.commitAsync();
        }
    }

    /*
     * 手动提交offset,并且手动控制partition
     * */
    private static void commitOffWithPartition() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false"); // 关闭自动提交
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅哪个topic
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            // 没间隔100毫秒去kafka拉取记录
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // 每个partition单独处理
            for(TopicPartition partition : records.partitions()) {

                // 单独拿出某个partition的所有数据，然后处理他们。
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    // todo 逻辑代码
                }

                // 单独针对指定的offset做提交
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();

                // 下一次消费服务器消息的时候，是这一次消费的最后的位置的下一个位置，所以需要+1
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(offset);
            }
        }
    }

    /*
     * 手动提交offset,并且手动控制partition(高级版)
     * 指订阅某个topic的某个partition
     * */
    private static void commitOffWithPartition2() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false"); // 关闭自动提交
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 两个线程处理两个partition
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 订阅某个topic的某个分区
        consumer.assign(List.of(p0));

        while (true) {
            // 没间隔100毫秒去kafka拉取记录
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // 每个partition单独处理
            for(TopicPartition partition : records.partitions()) {

                // 单独拿出某个partition的所有数据，然后处理他们。
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    // todo 逻辑代码
                }

                // 单独针对指定的offset做提交
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();

                // 下一次消费服务器消息的时候，是这一次消费的最后的位置的下一个位置，所以需要+1
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(offset);
            }
        }
    }

    /*
     * 手动控制offset起始位置
     * */
    private static void controlOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false"); // 关闭自动提交
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 两个线程处理两个partition
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 订阅某个topic的某个分区
        // 假如一次消费了100条，那么offset置伟101。然后存入redis，然后下次消费的时候在读出来，继续消费。
        consumer.assign(List.of(p0));

        while (true) {
            // 手动指定offset起始位置
            consumer.seek(p0, 400); // 从400开始消费

            // 没间隔100毫秒去kafka拉取记录
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // 每个partition单独处理
            for(TopicPartition partition : records.partitions()) {

                // 单独拿出某个partition的所有数据，然后处理他们。
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    // todo 逻辑代码
                }

                // 单独针对指定的offset做提交
                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();

                // 下一次消费服务器消息的时候，是这一次消费的最后的位置的下一个位置，所以需要+1
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(offset);
            }
        }
    }

    /*
    流量控制 - 限流
     */
    private static void controlPause() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        // jiangzh-topic - 0,1两个partition
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个Topic的某个分区
        consumer.assign(Arrays.asList(p0,p1));
        long totalNum = 40;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for(TopicPartition partition : records.partitions()){
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                long num = 0;
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                    /*
                        1、接收到record信息以后，去令牌桶中拿取令牌
                        2、如果获取到令牌，则继续业务处理
                        3、如果获取不到令牌， 则pause等待令牌
                        4、当令牌桶中的令牌足够， 则将consumer置为resume状态
                     */
                    num++;
                    if(record.partition() == 0){
                        if(num >= totalNum){
                            consumer.pause(List.of(p0));
                        }
                    }

                    if(record.partition() == 1){
                        if(num == 40){
                            consumer.resume(List.of(p0));
                        }
                    }
                }

                long lastOffset = pRecord.get(pRecord.size() -1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition,new OffsetAndMetadata(lastOffset+1));
                // 提交offset
                consumer.commitSync(offset);
                System.out.println("=============partition - "+ partition +" end================");
            }
        }
    }
}
