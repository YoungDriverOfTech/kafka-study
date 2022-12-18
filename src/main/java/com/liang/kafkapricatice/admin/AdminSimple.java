package com.liang.kafkapricatice.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class AdminSimple {

    public static void main(String[] args) {
        AdminClient adminClient = AdminSimple.adminClient();
        System.out.println("adminClient = " + adminClient);
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
