package com.yineng.stream.window;

import com.yineng.common.connectors.FlinkOrderSource;
import com.yineng.stream.pojo.Order;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;

/**
 * 统计每个商品，每个分类的销量和销售额
 * AggregateFunction 增量累加可以实现比ReduceFunction更加强大的计算， 结合WindowFunction或ProcessWindowFunction 可实现更为灵活和强大的计算（比如获取窗口、key的一些信息等）
 * 具体用法可查看 AggregateAndProcessWindowFunctionMain 和 AggregateAndWindowFunctionMain类
 */
public class AggregateFunctionMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Order> source = env.addSource(new FlinkOrderSource());
        source.print();
        source
                .keyBy("shopId", "categoryId")
                .timeWindow(Time.seconds(20))//20s的滚动窗口
                .aggregate(new AggregateFunction<Order, Tuple4<Long, Long, Long, BigDecimal>, Tuple4<Long, Long, Long, BigDecimal>>() {
                    //数据初始化
                    @Override
                    public Tuple4<Long, Long, Long, BigDecimal> createAccumulator() {
                        return Tuple4.of(0L, 0L, 0L, BigDecimal.valueOf(0));
                    }

                    //累加计算，每个数据流都会经过这个方法，输入数据为 每个数据流和 之前的累加结果
                    @Override
                    public Tuple4<Long, Long, Long, BigDecimal> add(Order order, Tuple4<Long, Long, Long, BigDecimal> acc) {
                        long shopId = acc.f0;
                        if (shopId == 0) {
                            shopId = order.getShopId();
                        }
                        long categoryId = acc.f1;
                        if (categoryId == 0) {
                            categoryId = order.getCategoryId();
                        }
                        return Tuple4.of(shopId, categoryId, acc.f2+1, acc.f3.add(order.getAmount()));
                    }
                    //获取结果，可以对结果在进行一个处理
                    @Override
                    public Tuple4<Long, Long, Long, BigDecimal> getResult(Tuple4<Long, Long, Long, BigDecimal> accumulator) {
                        return accumulator;
                    }
                    //多个分区的迭代数据如何合并 只有在会话窗口调用
                    @Override
                    public Tuple4<Long, Long, Long, BigDecimal> merge(Tuple4<Long, Long, Long, BigDecimal> acc1, Tuple4<Long, Long, Long, BigDecimal> acc2) {
                        return null;
                    }
                })
                .printToErr();
        env.execute("aggregateFunction");
    }
}
