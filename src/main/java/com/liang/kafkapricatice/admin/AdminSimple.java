package com.liang.kafkapricatice.admin;

import org.apache.kafka.clients.admin.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class AdminSimple {

    public final static String TOPIC_NAME = "jz_topic";


    public static void main(String[] args) throws Exception {

        // 1. 创建kafka集群
//        AdminClient adminClient = AdminSimple.adminClient();
//        System.out.println("adminClient = " + adminClient);

        // 2. 创建topic实例
//        createTopic();

        // 3. 获取topic名字
//        topicLists();

        // 4. 删除topic
//        deleteTopic();

        // 5. 描述topic
        // {first-topic=
        // (name=first-topic,
        // internal=false,
        // partitions=(partition=0, leader=ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092 (id: 0 rack: null),
        // replicas=ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092 (id: 0 rack: null),
        // isr=ec2-35-73-156-207.ap-northeast-1.compute.amazonaws.com:9092 (id: 0 rack: null)),
        // authorizedOperations=[])}
        describeTopic();
    }

    /*
    * 描述topic
    * */
    private static void describeTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(List.of("first-topic"));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        System.out.println("stringTopicDescriptionMap = " + stringTopicDescriptionMap);
    }

    /*
    * 删除topic
    * */
    private static void deleteTopic() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(List.of(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }


    /*
    * kafka查找
    * */
    public static void topicLists() throws Exception {
        AdminClient adminClient = adminClient();

        // 把内部topic一起打印出来
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        // 不加options，不打印内部topic
//        ListTopicsResult listTopicsResult = adminClient.listTopics();

        // 加上options，把内部topic也能打出来
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        Set<String> names = listTopicsResult.names().get();
        names.forEach(System.out::println);
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
