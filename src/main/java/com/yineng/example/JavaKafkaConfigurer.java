package com.yineng.example;

import java.util.Properties;

import static com.yineng.common.constants.PropertiesConstants.APP_CONFIG_NAME;

public class JavaKafkaConfigurer {

    private static Properties properties;

    public synchronized static Properties getKafkaProperties() {
        if (null != properties) {
            return properties;
        }
        //获取配置文件 kafka.properties 的内容
        Properties kafkaProperties = new Properties();
        try {
            kafkaProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_CONFIG_NAME));
//            kafkaProperties.load(KafkaProducerDemo.class.getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (Exception e) {
            //没加载到文件，程序要考虑退出
            e.printStackTrace();
        }
        properties = kafkaProperties;
        return kafkaProperties;
    }
}
