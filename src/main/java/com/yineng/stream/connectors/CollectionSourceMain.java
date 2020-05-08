package com.yineng.stream.connectors;

import com.google.common.collect.Lists;
import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

@Slf4j
public class CollectionSourceMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //fromElements 从一个给定的对象序列中创建一个数据流，所有的对象必须是相同类型的。 POJO 基本类型 和 Tuple等
        env.setParallelism(1);
        //POJO
        env.fromElements(
                new Order(1,100, 10001 , 11, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:11"))),
                new Order(2,101, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:21"))),
                new Order(3,100, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:31"))),
                new Order(2,102, 10001 , 11, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:41"))),
                new Order(1,103, 10003 , 13, BigDecimal.valueOf(103), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:51")))

        ).print();

        //基本类型
        env.fromElements(1, 2, 3, 5, 1, 3, 6, 9, 10).print();
        //Tuple
        env.fromElements(
                Tuple4.of(1, 100, 1000, 9.99),
                Tuple4.of(1, 101, 1001, 9.99),
                Tuple4.of(3, 101, 1002, 9.99),
                Tuple4.of(4, 100, 1001, 9.99),
                Tuple4.of(6, 100, 1001, 9.99)

        ).print();//sink 标准输出

        //从 fromCollection 集合创建数据流
        List<Tuple4> studentList = Lists.newArrayList(
                Tuple4.of(2, 100, 1000, 9.99),
                Tuple4.of(2, 101, 1001, 9.99)
        );
        DataStreamSource<Tuple4> tuple4DataStreamSource = env.fromCollection(studentList);
        //sink 标准错误输出
        tuple4DataStreamSource.printToErr();
//        tuple4DataStreamSource.writeAsCsv("./aa.csv",  FileSystem.WriteMode.OVERWRITE);
        //基于给定的序列区间进行构建
        env.generateSequence(0,100).printToErr();

        env.execute("predefined source");
    }
}
