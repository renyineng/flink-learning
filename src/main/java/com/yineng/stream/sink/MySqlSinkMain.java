package com.yineng.stream.sink;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class MySqlSinkMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //POJO
        DataStreamSource<Order> source = env.fromElements(
                new Order(1,100, 10001 , 11, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:11"))),
                new Order(2,101, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:21"))),
                new Order(3,100, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:31"))),
                new Order(2,102, 10001 , 11, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:41"))),
                new Order(1,103, 10003 , 13, BigDecimal.valueOf(103), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:51")))

        );
        source.print();//标准输出
        source.printToErr();//标准错误输出
        env.execute("sink to mysql example");
    }
}
