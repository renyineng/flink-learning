package com.yineng.stream.connectors.custom;

import com.alibaba.fastjson.JSONObject;
import com.yineng.stream.pojo.Heart;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * 自定义soure 每2秒产生一条心跳数据
 */
@Slf4j
public class SourceFromFunctionMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Heart> source = env.addSource(new SourceFunction<Heart>() {
            private boolean isRunning = true;
            private int index = 1;
            @Override
            public void run(SourceContext<Heart> ctx) throws Exception {
                //每2s 产生一条心跳数据
                while (isRunning) {
                    if (index < 100) {
                        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
                        Heart heart = new Heart("project"+index, index, index+"", timestamp);
                        ctx.collect(heart);
                        index++;
                        Thread.sleep(2000);
                    }  else {
                        isRunning = false;
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


//        printToErr();
        env.execute("diy source");
    }
}
