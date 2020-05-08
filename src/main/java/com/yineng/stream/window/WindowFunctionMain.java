package com.yineng.stream.window;

import com.alibaba.fastjson.JSONObject;
import com.yineng.common.connectors.FlinkOrderSource;
import com.yineng.stream.pojo.Order;
import com.yineng.stream.pojo.OrderAccumulator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * 此函数可以被ProcessWindowFunction 代替，缺少状态等一些信息，官方说 将来会被废弃，因为也建议直接使用ProcessWindowFunction
 * 窗口函数之WindowFunction 对应apply
 * 数据源 POJO Order {"shop_id":1,"category_id":11,"goods_id":10001,"user_id":100,"amount":100,"created_at":"2020-01-01 00:00:11"}
 */
public class WindowFunctionMain {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Order> source = env.addSource(new FlinkOrderSource());
        source.print();
        source
            .keyBy("shopId", "categoryId")
    //        .keyBy(Order::getShopId, Order::getCategoryId)
            .timeWindow(Time.seconds(10))
            .apply(new MyWindowFunction())
            .print();
        env.execute("windowfunction");
    }

    /**
     * 窗口处理，窗口结束时触发
     */
    public static class MyWindowFunction implements WindowFunction<Order, OrderAccumulator, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Order> iterable, Collector<OrderAccumulator> collector) throws Exception {
            Tuple2<Long, Long> key = (Tuple2) tuple;
            System.err.println(key+"key");
            Timestamp start = new Timestamp(timeWindow.getStart());
            Timestamp end = new Timestamp(timeWindow.getEnd());
            OrderAccumulator orderAccumulator = new OrderAccumulator();
            orderAccumulator.setShopId(key.f0);
            orderAccumulator.setCategoryId(key.f1);
            ArrayList<Long> userList = new ArrayList<>();
            long count = 0;
            BigDecimal sumAmount = BigDecimal.valueOf(0);
            for(Order order: iterable) {
                boolean isContains = userList.contains(order.getUserId());
                if (!isContains) {
                    userList.add(order.getUserId());
                }
                sumAmount = sumAmount.add(order.getAmount());
                count++;
            }
            orderAccumulator.setUserCount(userList.size());
            orderAccumulator.setAmount(sumAmount);
            orderAccumulator.setCount(count);
            orderAccumulator.setStartWindow(start);
            orderAccumulator.setEndWindow(end);
            collector.collect(orderAccumulator);
        }
    }
}
