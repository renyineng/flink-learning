package com.yineng.stream.connectors.custom;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

import static com.yineng.common.constants.PropertiesConstants.APP_CONFIG_NAME;

/**
 * 从kafka读取数据
 */
@Slf4j
public class SourceFromKafkaMain {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool
                .fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_CONFIG_NAME))
                .mergeWith(ParameterTool.fromArgs(args));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 每隔 5000 毫秒 执行一次 checkpoint
//        如果未启用 checkpoint，那么 Kafka consumer 将定期向 Zookeeper 提交 offset。
//        String group = parameter.get("group");
        String topic = "binlog_test";
        System.err.println("test11");
        Properties props = new Properties();
        String kafkaUser = "GLOkafkatest";
        String kafkaPassword = "GLOkafka2019";
        log.error(kafkaUser+"kafkaUser");
        if (kafkaUser!=null) {

            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required\nusername=\"%s\"\npassword=\"%s\";";
            String jaasConfig = String.format(jaasTemplate, kafkaUser, kafkaPassword);
            props.put("sasl.jaas.config", jaasConfig);
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, parameter.get("ssl.truststore.location"));
            //根证书 store 的密码，保持不变
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
            //接入协议，目前支持使用 SASL_SSL 协议接入
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            //SASL 鉴权方式，保持不变
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        }
        props.setProperty("bootstrap.servers", parameter.get("kafka.bootstrap.servers"));
        props.setProperty("group.id", "glo_binlog_test");
        props.put("enable.auto.commit", "false");//不需要自动提交offset， flink状态里会自动保存当前消费的位置
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
//        当 Job 从故障中自动恢复或使用 savepoint 手动恢复时，这些起始位置配置方法不会影响消费的起始位置。在恢复时，每个 Kafka 分区的起始位置由存储在 savepoint 或 checkpoint 中的 offset 确定
//        如果 Job 失败，Flink 会将流式程序恢复到最新 checkpoint 的状态，并从存储在 checkpoint 中的 offset 开始重新消费 Kafka 中的消息。
//因此，设置 checkpoint 的间隔定义了程序在发生故障时最多需要返回多少
//        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
//        从最早或者最新的记录开始消费，在这些模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置。
//        kafkaConsumer.setStartFromEarliest();//尽可能从最早的记录开始
//        kafkaConsumer.setStartFromLatest();//从最新的位置开始消费
//        kafkaConsumer.setStartFromTimestamp(1585026056000L);//从指定毫秒处消费
        // 从消费组位置开发消费 默认方法 如果未找到位置 则以auto.offset.reset 设置的为准 有earliest,latest.none
        kafkaConsumer.setStartFromGroupOffsets();//默认的模式 ，经测试不设置group.id 也可以使用这个模式的， 可以理解为找不到位置 从auto.offset.reset 处开始消费，一般默认为latest

        DataStreamSource<String> source = env.addSource(kafkaConsumer);
        source.print();
        env.execute("from kafka source");
    }
//
}
