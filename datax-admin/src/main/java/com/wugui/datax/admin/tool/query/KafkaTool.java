package com.wugui.datax.admin.tool.query;


import com.wugui.datax.admin.entity.JobDatasource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.kafka.core.KafkaAdmin;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaTool {
    private String url;

    public KafkaTool(JobDatasource jobDatasource) {
        url=jobDatasource.getJdbcUrl();
    }
    public KafkaTool(String lurl) {
        url=lurl;
    }
    public boolean dataSourceTest(){
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,3000);
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, url);
        AdminClient adminClient = AdminClient.create(properties);
        try{
            String id=adminClient.describeCluster().clusterId().get();
            return true;
        }catch (Exception e){
            return false;
        }
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaTool kafkaTool=new KafkaTool("172.16.20.71:9092,172.16.20.70:9092");
        System.out.println(kafkaTool.dataSourceTest());
//        Map<String, Object> configs = new HashMap<>();
//        KafkaAdmin kafkaAdmin;
//        KafkaAdminClient kafkaAdminClient=new KafkaAdminClient();
//        kafkaAdminClient.li
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
//                StringUtils.arrayToCommaDelimitedString(kafkaEmbedded().getBrokerAddresses()));
//        KafkaAdmin kafkaAdmin=KafkaAdmin(configs);
    }
}
