package com.yineng.stream.window;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Heart;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;

@Slf4j
public class Window {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从一个给定的对象序列中创建一个数据流，所有的对象必须是相同类型的。
        // fromElements 支持 基本类型 和POJO
        DataStreamSource<Heart> source = env.fromElements(
                    new Heart("project1",1, "100", Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:11"))),
                    new Heart("project1",2, "100", Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:21"))),
                    new Heart("project1",2, "101", Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:31")))
        );
        source.print();

//        DataStreamSource<Integer> sourceInt = env.fromElements(1, 2, 3,5,1,3, 6, 9, 10);
        //数据源， 用户id ，分类，商品id， 价格
        // 统计每个分类的销量和销售额
        DataStreamSource<Tuple4<Integer, Integer, Integer, Double>> sourceTuple = env.fromElements(

                Tuple4.of(1, 100, 1000, 9.99),
                Tuple4.of(1, 101, 1001,9.99),
                Tuple4.of(3, 101, 1002, 9.99),
                Tuple4.of(4, 100, 1001, 9.99),
                Tuple4.of(6, 100, 1001, 9.99)

        );
        sourceTuple.keyBy(1)
                .sum(3).printToErr();
//        sourceTuple.windowAll(WindowAssigner).sum(1);


//        sourceInt.map(i-> new Tuple2<>(i, 1))
//                .returns(Types.TUPLE(Types.INT, Types.INT))
//                .keyBy(0)
//                .sum(1)
//                .printToErr();
//
//        source.keyBy("userId").minBy("time").printToErr();
//
        env.execute("predefined source");
//        int res = JobExecutionResult.getAccumulatorResult("num-lines");
//        System.err.println(res+"res");
//
    }
}
