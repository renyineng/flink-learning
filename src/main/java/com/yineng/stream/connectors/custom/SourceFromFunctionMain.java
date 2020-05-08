package com.yineng.stream.connectors.custom;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
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
        DataStreamSource<Order> source = env.addSource(new SourceFunction<Order>() {
            private boolean isRunning = true;
            private int index = 1;
            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                //每2s 产生一条心跳数据
                while (isRunning) {
                    if (index < 100) {
//                        LocalDateTime.now();
                        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
//                        Order heart = new Order("project"+index, index, index+"", BigDecimal.valueOf(index), timestamp);
                        Order order = new Order(1,index, index , 11, BigDecimal.valueOf(100), timestamp);
                        ctx.collect(order);
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
