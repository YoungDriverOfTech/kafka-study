package com.liang.kafkapricatice.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    public static final String TOPIC_NAME = "first-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 1. Producer 异步发送消息
//        producerSend();

        // 2. 同步发送
//        producerSendSync();

        // 3. 异步回调
        producerSendWithCallback();
    }

    /*
     * 异步回调
     * */
    public static void producerSendWithCallback() {
        // 配置kafka的地址
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Producer 的消息对象 Producer Record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);

            // 回调函数传入send方法
            producer.send(record, (metadata, exception) -> {

                // 这里面是回调的实现
                System.out.println("partition = " + metadata.partition() + "  offset+" + metadata.offset());
            });
        }

        // 所有的通道打开，都需要关闭
        producer.close();
    }


    /*
     * Producer同步发送
     * */
    public static void producerSendSync() throws ExecutionException, InterruptedException {
        // 配置kafka的地址
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Producer 的消息对象 Producer Record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.out.println("partition = " + recordMetadata.partition() + "  offset+" + recordMetadata.offset());
        }

        // 所有的通道打开，都需要关闭
        producer.close();
    }

    /*
    * Producer异步发送
    * */
    public static void producerSend() {
        // 配置kafka的地址
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Producer 的消息对象 Producer Record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer.send(record);
        }

        // 所有的通道打开，都需要关闭
        producer.close();
    }
}
