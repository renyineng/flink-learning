package com.yineng.stream.window;

import com.yineng.common.connectors.FlinkOrderSource;
import com.yineng.stream.pojo.Order;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * AllWindow用法 结合process算子
 * DataStream → AllWindowedStream → DataStream
 * 每5分钟 输出 开始时间 结束时间 统计订单数 和订单金额
 */
public class AllWindowMain {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<Order> source = env.addSource(new FlinkOrderSource());//每2s产生一条订单数据
//        source.print();
        SingleOutputStreamOperator<Tuple4<Timestamp, Timestamp, Long, BigDecimal>> process = source
            .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5)))
//                .timeWindowAll(Time.seconds(10))
            .process(new ProcessAllWindowFunction<Order, Tuple4<Timestamp, Timestamp, Long, BigDecimal>, TimeWindow>() {
                @Override
                public void process(Context context, Iterable<Order> iterable, Collector<Tuple4<Timestamp, Timestamp, Long, BigDecimal>> collector) throws Exception {
                    System.err.println(iterable);//窗口内元素迭代器
                    long count = 0;
                    BigDecimal orderSum = BigDecimal.valueOf(0);

                    for (Order order: iterable) {
                        count++;
                        orderSum = orderSum.add(order.getAmount());
                    }

                    //窗口开始和结束时间
                    Timestamp startTime = new Timestamp(context.window().getStart());
                    Timestamp endTime = new Timestamp(context.window().getEnd());
                    collector.collect(Tuple4.of(startTime, endTime, count, orderSum));
                }
            });
        process.printToErr();
        env.execute("windowfunction");
    }
}
