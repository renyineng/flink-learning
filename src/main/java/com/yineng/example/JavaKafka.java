package com.yineng.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//java kafka 客户端消费数据，通过本例子来区别 flink kafka客户端和java kafka客户端的区别
// 同一个消费者组 消费同一个topic 并不会重复消费，
// 一个分区的消息 只能被一个消费者消费  这点和flink 的kafka客户端不同，flink的kafka消费者并不依赖offset,提交的 offset 只是一种方法，用于公开 consumer 的进度以便进行监控。
public class JavaKafka {
    public static void main(String[] args) {
        //加载 kafka.properties
        Properties kafkaProperties =  JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应 Topic 的接入点
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("kafka.bootstrap.servers"));
        //默认值为 30000 ms，可根据自己业务场景调整此值，建议取值不要太小，防止在超时时间内没有发送心跳导致消费者再均衡
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 25000);
        //每次 poll 的最大数量
        //注意该值不要改得太大，如果 poll 太多数据，而不能在下次 poll 之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        //消息的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //当前消费实例所属的 Consumer Group，请在控制台创建后填写
        //属于同一个 Consumer Group 的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        //设置  Consumer Group 订阅的 Topic，可订阅多个 Topic。如果 GROUP_ID_CONFIG 相同，那建议订阅的 Topic 设置也相同
        List<String> subscribedTopics =  new ArrayList<String>();
        //每个 Topic 需要先在控制台进行创建
        subscribedTopics.add("binlog_test");
        String kafkaUser = "";
        String kafkaPassword = "";
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required\nusername=\"%s\"\npassword=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, kafkaUser, kafkaPassword);
        props.put("sasl.jaas.config", jaasConfig);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaProperties.get("ssl.truststore.location"));
        //根证书 store 的密码，保持不变
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
        //接入协议，目前支持使用 SASL_SSL 协议接入
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        //SASL 鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        //构造消息对象，即生成一个消费实例
        KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);

        consumer.subscribe(subscribedTopics);

        //循环消费消息
        while (true){
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                //必须在下次 poll 之前消费完这些数据, 且总耗时不得超过 SESSION_TIMEOUT_MS_CONFIG 的值
                //建议开一个单独的线程池来消费消息，然后异步返回结果
//                System.err.println(records+"records");
                for (ConsumerRecord<String, String> record : records) {
                    System.err.println(String.format("Consume partition:%d offset:%d", record.partition(), record.offset()));
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable ignore) {

                }
                //更多报错信息，参见常见问题文档
                e.printStackTrace();
            }
        }
    }

}
