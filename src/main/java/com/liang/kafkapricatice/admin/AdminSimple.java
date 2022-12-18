package com.liang.kafkapricatice.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

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
//        describeTopic();

        // 6. 描述配置
        // {ConfigResource(type=TOPIC, name='first-topic')=
        // Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
        // ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])}
        describeConfig();
    }

    /*
    * 查看config
    * */
    private static void describeConfig() throws Exception {
        AdminClient adminClient = adminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "first-topic");
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(List.of(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();

        System.out.println("configResourceConfigMap = " + configResourceConfigMap);
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
