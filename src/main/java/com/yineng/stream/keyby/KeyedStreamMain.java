package com.yineng.stream.keyby;

import com.alibaba.fastjson.JSONObject;
import com.yineng.common.connectors.FlinkOrderSource;
import com.yineng.common.utils.DateUtils;
import com.yineng.common.utils.ExecutionEnvUtil;
import com.yineng.stream.pojo.HeartLog;
import com.yineng.stream.pojo.LiveOnlineUserStat;
import com.yineng.stream.pojo.Order;
import com.yineng.stream.pojo.OrderAccumulator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class KeyedStreamMain {
    public static void main(String[] args) throws Exception {
        // 初始化参数
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        // 创建env环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        DataStreamSource<Order> source = env.addSource(new FlinkOrderSource());
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
        SingleOutputStreamOperator<Tuple4<Long, Long,  Long, BigDecimal>> shopStatStream = source.keyBy("shopId", "categoryId")
                .process(new KeyedProcessFunction<Tuple, Order, Tuple4<Long, Long,  Long, BigDecimal>>() {//为了简化代码 使用了tuple 类型 使用pojo 会更加清晰
                    private ValueState<Tuple4<Long, Long,  Long, BigDecimal>> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Tuple4<Long, Long,  Long, BigDecimal>> shopOrderStat = new ValueStateDescriptor<>("shopOrderStat", Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.BIG_DEC));
                        state = getRuntimeContext().getState(shopOrderStat);
                    }


                    /**
                     * 处理分组内每一个事件，一般配合定时器或者状态计算 比如MapState、ValueState等
                     * Order order 为当前数据流
                     */
                    @Override
                    public void processElement(Order order, Context context, Collector<Tuple4<Long, Long,  Long, BigDecimal>> collector) throws Exception {
                        Tuple4<Long, Long,  Long, BigDecimal> current = state.value();
                        System.err.println(current);
                        Tuple2<Long, Long> currentKey = (Tuple2) context.getCurrentKey();
                        if (current == null) {//当前key的第一条数据 初始化
                            current =Tuple4.of(currentKey.f0, currentKey.f1, 0L, BigDecimal.valueOf(0));
                        }
                        //销售额
                        BigDecimal shopAmountTotal = current.f3.add(order.getAmount());
                        //订单数
                        long orderCount = current.f2+1;
                        current = Tuple4.of(currentKey.f0, currentKey.f1, orderCount, shopAmountTotal);
                        state.update(current);
                        collector.collect(current);
                    }
                });
        source.print();
        shopStatStream.printToErr();
        env.execute("keyby learn");
    }
}
