package com.liang.kafkapricatice.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AdminSimple {

    public final static String TOPIC_NAME = "jz_topic";


    public static void main(String[] args) {

        // 1. 创建kafka集群
//        AdminClient adminClient = AdminSimple.adminClient();
//        System.out.println("adminClient = " + adminClient);

        // 2. 创建topic实例
        createTopic();
    }

    /*
    * 创建Topic实例
    * */
    public static void createTopic() {
        AdminClient adminClient = AdminSimple.adminClient();

        // 副本因子
        short rs = 1;

        // 第二个参数是分区数量，第三个是副本数量
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, rs);

        // createTopics接受数组
        CreateTopicsResult topics = adminClient.createTopics(List.of(newTopic));
        System.out.println("topics = " + topics);
    }

    /*
    * 设置admin client
    * */
    public static AdminClient adminClient() {
        // kafka的配置
        Properties properties = new Properties();

        // 集群地址
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092");

        return AdminClient.create(properties);
    }
}
