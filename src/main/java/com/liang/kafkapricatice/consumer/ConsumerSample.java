package com.liang.kafkapricatice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerSample {

    private static final String TOPIC_NAME = "first-topic";

    public static void main(String[] args) {

        // 1. 最简单的消费者
        helloWorld();
    }

    /*
    * 最简单的消费者
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
}
