package com.yineng.common.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

//import static com.yineng.flink.common.constants.PropertiesConstants.*;
import static com.yineng.common.constants.PropertiesConstants.APP_ENV;

public class KafkaSource {
    private ParameterTool parameter;
    private Properties props;
    //key 为 topic consume_type 当为time 时 必须有start_time
    public static KafkaSource builder(ParameterTool parameterTool) {
        KafkaSource kafkaSource = new KafkaSource();
        kafkaSource.parameter = parameterTool;
        kafkaSource.props = kafkaSource.setProps();
        return kafkaSource;
    }

    public DataStreamSource<String> readSource(StreamExecutionEnvironment env, String topic) throws Exception {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();

        if (null == parameter.get("group")) {
            throw new Exception("group required");
        }
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);
        String consumeType = parameter.get("consume_type");
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        //默认从最新位置开始消费 可以指定earliest 或者time
        if("earliest".equals(consumeType)) {
            kafkaConsumer.setStartFromEarliest();
        } else if("time".equals(consumeType)) {
            kafkaConsumer.setStartFromTimestamp(parameter.getLong("start_time"));
        } else {
            kafkaConsumer.setStartFromLatest();
        }
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        return source;
    }
    public DataStreamSource<String> readSource(StreamExecutionEnvironment env) throws Exception {
        String topic = parameter.get("topic");
        if (null == topic) {
            throw new Exception("topic required");
        }
        return readSource(env, topic);
    }
    public Properties setProps() {

        Properties props = new Properties();
        String appEnv = parameter.get(APP_ENV);
        if ("local".equals(appEnv)) {
            String kafkaPassword = parameter.get("kafka_password");
            String kafkaUser = parameter.get("kafka_user");
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
        //请求的最长等待时间
        props.setProperty("bootstrap.servers", parameter.get("kafka.bootstrap.servers"));
        props.put("enable.auto.commit", "false");
        props.setProperty("group.id", parameter.get("group"));

        return props;
    }
    //数据写入
    public FlinkKafkaProducer011 write(String topic) {
        return new FlinkKafkaProducer011<>(topic, new SimpleStringSchema(), props);
    }

}
