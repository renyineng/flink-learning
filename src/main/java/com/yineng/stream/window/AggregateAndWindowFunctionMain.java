package com.yineng.stream.window;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import com.yineng.stream.pojo.OrderAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * aggregate结合WindowFunction例子
 */
@Slf4j
public class AggregateAndWindowFunctionMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Order heart) {
                return heart.getCreatedAt().getTime();
            }
        })
        .keyBy(new KeySelector<Order, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(Order heart) throws Exception {
                return Tuple2.of(heart.getShopId(), heart.getCategoryId());
            }
        })
        .timeWindow(Time.seconds(60))
//        .trigger(CountTrigger.of(1))
        .aggregate(new MyAggregateFunction(), new MyWindowFunction())
        .printToErr();
        env.execute("predefined source");

//
    }

    public static class MyAggregateFunction implements AggregateFunction<Order, Tuple2<Long, BigDecimal>, Tuple2<Long, BigDecimal>> {
//        迭代状态的初始值
        @Override
        public Tuple2<Long, BigDecimal> createAccumulator() {
            return Tuple2.of(0L, BigDecimal.valueOf(0));
        }

        @Override
//        每一条输入数据，和迭代数据如何迭代
        public Tuple2<Long, BigDecimal> add(Order order, Tuple2<Long, BigDecimal> acc) {
            return Tuple2.of(acc.f0+1, acc.f1.add(order.getAmount()));
        }

        @Override
        //返回数据，对最终的迭代数据如何处理，并返回结果。
        public Tuple2<Long, BigDecimal> getResult(Tuple2<Long, BigDecimal> acc) {
            return Tuple2.of(acc.f0, acc.f1);
        }

        @Override
        // 每个分区数据之间如何合并数据
        public Tuple2<Long, BigDecimal> merge(Tuple2<Long, BigDecimal> a, Tuple2<Long, BigDecimal> b) {
//            a.add(b);
            return Tuple2.of(a.f0+b.f0, a.f1.add(b.f1));
        }
    }
    public static class MyWindowFunction implements WindowFunction<Tuple2<Long, BigDecimal>, OrderAccumulator, Tuple2<Long, Long>, TimeWindow> {
        @Override
        public void apply(Tuple2<Long, Long> key, TimeWindow timeWindow, Iterable<Tuple2<Long, BigDecimal>> input, Collector<OrderAccumulator> collector) throws Exception {

            OrderAccumulator orderAccumulator = new OrderAccumulator();
            orderAccumulator.setShopId(key.f0);
            orderAccumulator.setCategoryId(key.f1);
            orderAccumulator.setAmount(input.iterator().next().f1);
            orderAccumulator.setCount(input.iterator().next().f0);
            orderAccumulator.setStartWindow(new Timestamp(timeWindow.getStart()));
            orderAccumulator.setEndWindow(new Timestamp(timeWindow.getEnd()));
            collector.collect(orderAccumulator);
        }

//        @Override
//        public void process(Long key, Context context, Iterable<BigDecimal> iterable, Collector<OrderAccumulator> collector) throws Exception {
//            OrderAccumulator orderAccumulator = new OrderAccumulator();
//            long start = context.window().getStart();
//            orderAccumulator.setShopId(key);
////            orderAccumulator.setGoodsId(key);
//            orderAccumulator.setStartWindow(new Timestamp(start));
//            orderAccumulator.setEndWindow(new Timestamp(context.window().getEnd()));
//            collector.collect(orderAccumulator);
//        }
    }
}
