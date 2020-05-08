package com.yineng.stream.window;

import com.alibaba.fastjson.JSONObject;
import com.yineng.stream.pojo.Order;
import com.yineng.stream.pojo.OrderAccumulator;
import lombok.extern.slf4j.Slf4j;
import java.sql.Timestamp;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * ProcessWindowFunction 例子
 */
@Slf4j
public class ProcessWindowFunctionMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //从socket中读取数据源  需要先执行 nc -l 9000 ,否则会拒接链接
        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
        SingleOutputStreamOperator<Order> source = text.map(s -> JSONObject.parseObject(s, Order.class));
        source.print();
        source
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Order order) {
                        return order.getCreatedAt().getTime();
                    }
            })
            .keyBy("shopId","categoryId")
            .timeWindow(Time.seconds(20))
            .process(new MyProcessWindowFunction())
            .printToErr();

        env.execute();
    }
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Order, OrderAccumulator, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Order> iterable, Collector<OrderAccumulator> collector) throws Exception {
            Tuple2<Long, Long> key = (Tuple2) tuple;
            System.err.println(context.currentWatermark()+"watermark");
            System.err.println(context.globalState()+"globalstate");
            System.err.println(context.windowState()+"windowState");
            Timestamp start = new Timestamp(context.window().getStart());
            Timestamp end = new Timestamp(context.window().getEnd());
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
