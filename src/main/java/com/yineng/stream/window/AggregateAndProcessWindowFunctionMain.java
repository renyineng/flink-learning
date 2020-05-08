package com.yineng.stream.window;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import com.yineng.stream.pojo.OrderAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * 统计最近60s 各个商家各个分类的销售额和销量
 * aggregate 只能应用于WindowStream
 * Aggregate 结合 ProcessWindowFunction 实现更加复杂的累加计算并获取一些窗口的其他信息
 * Aggregate 单独使用 参考 AggregateFunctionMain类
 * ProcessWindowFunction 单独使用 参考 ProcessWindowFunctionMain类
 */
@Slf4j
public class AggregateAndProcessWindowFunctionMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //从一个给定的对象序列中创建一个数据流，所有的对象必须是相同类型的。
        // fromElements 支持 基本类型 和POJO
        DataStreamSource<Order> source = env.fromElements(
                new Order(1,100, 10001 , 11, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:11"))),
                new Order(1,103, 10003 , 13, BigDecimal.valueOf(103), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:51"))),
                new Order(2,101, 10002 , 15, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:21"))),
                new Order(2,102, 10001 , 15, BigDecimal.valueOf(100), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:41"))),
                new Order(3,100, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:31"))),
                new Order(3,101, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:00:31"))),
                new Order(3,102, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:02:35"))),
                new Order(3,103, 10002 , 12, BigDecimal.valueOf(102), Timestamp.valueOf(DateUtils.parse("2020-01-01 00:02:38")))
        );
        source
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {//允许数据迟到10s
            @Override
            public long extractTimestamp(Order heart) {
                return heart.getCreatedAt().getTime();//提取事件时间
            }
        })
        .keyBy(new KeySelector<Order, Tuple2<Long, Long>>() {
            @Override
            //按照商家id 和分类id聚合
            public Tuple2<Long, Long> getKey(Order heart) throws Exception {
                return Tuple2.of(heart.getShopId(), heart.getCategoryId());
            }
        })
        .timeWindow(Time.seconds(60))
//        .trigger(CountTrigger.of(1))
        .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction())
        .printToErr();
        env.execute("predefined source");

//
    }
    public static class MyProcessWindowFunction extends ProcessWindowFunction<OrderAccumulator, OrderAccumulator, Tuple2<Long, Long>, TimeWindow> {
        @Override
        public void process(Tuple2<Long, Long> key, Context context, Iterable<OrderAccumulator> iterable, Collector<OrderAccumulator> collector) throws Exception {

            OrderAccumulator next = iterable.iterator().next();

            OrderAccumulator orderAccumulator = new OrderAccumulator();
            orderAccumulator.setShopId(key.f0);
            orderAccumulator.setCategoryId(key.f1);
            orderAccumulator.setCount(next.getCount());
            orderAccumulator.setAmount(next.getAmount());
            long start = context.window().getStart();
            orderAccumulator.setStartWindow(new Timestamp(start));
            orderAccumulator.setEndWindow(new Timestamp(context.window().getEnd()));
            collector.collect(orderAccumulator);
        }
    }
    public static class MyAggregateFunction implements AggregateFunction<Order, OrderAccumulator, OrderAccumulator> {
//        迭代状态的初始值
        @Override
        public OrderAccumulator createAccumulator() {
            return new OrderAccumulator();
        }
        @Override
//        每一条输入数据，和迭代数据如何迭代
        public OrderAccumulator add(Order order, OrderAccumulator acc) {
            acc.setCount(acc.getCount() +1);//销售量+1
            acc.setAmount(acc.getAmount().add(order.getAmount()));//销售额累加
            return acc;
        }

        @Override
        //返回数据，对最终的迭代数据如何处理，并返回结果。
        public OrderAccumulator getResult(OrderAccumulator accumulator) {
            return accumulator;
        }

        @Override
        // 每个分区数据之间如何合并数据
        public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
            a.setAmount(a.getAmount().add(b.getAmount()));
            a.setCount(a.getCount() + b.getCount());
            return a;
        }
    }
}
