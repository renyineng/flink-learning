package com.yineng.stream.window;

import com.alibaba.fastjson.JSONObject;
import com.sun.javadoc.Type;
import com.yineng.common.connectors.FlinkOrderSource;
import com.yineng.stream.pojo.Order;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;

/**
 * ReduceFunction 学习
 * 接收两个类型相同的数据，并将其结合输出为一个类型相同的值
 * 当有新数据加入窗口时：该函数被调用，并传入新数据和窗口状态值作为参数。
 * 优点：
 *  窗口状态恒定而且数据量小；
 *  接口简单；
 * 缺点：
 * 输入输出类型必须相同，可用于简单的聚合需求
 * https://xieyuanpeng.com/2019/02/21/flink-learning-4/
 * timeWindow 只能用于keyedStream
 * demo：统计各个商家的累计销售额和销售量
 * 注意点： 此处的map 用了lambda  则必须使用returns 强调返回的类型，否则会报错
 */
public class ReduceFunctionMain {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
        SingleOutputStreamOperator<Tuple3<Long, BigDecimal, Long>> source = text
                .map(s -> {
                    JSONObject json = JSONObject.parseObject(s);
                    return Tuple3.of(json.getLongValue("shop_id"), json.getBigDecimal("amount"), 1L);
                }).returns(Types.TUPLE(Types.LONG, Types.BIG_DEC, Types.LONG));
        SingleOutputStreamOperator<Tuple3<Long, BigDecimal, Long>> reduce = source
            .keyBy(0)
//                .timeWindow(Time.seconds(10))
            .reduce(new ReduceFunction<Tuple3<Long, BigDecimal, Long>>() {
                @Override
                public Tuple3<Long, BigDecimal, Long> reduce(Tuple3<Long, BigDecimal, Long> t1, Tuple3<Long, BigDecimal, Long> t2) throws Exception {
                    return Tuple3.of(t1.f0, t1.f1.add(t2.f1), t1.f2+1L);
                }
            });
        reduce.printToErr();
        env.execute("reduce compute");
    }
}
