package com.yineng.common.connectors;

import com.yineng.common.utils.DateUtils;
import com.yineng.stream.pojo.Order;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;

public class FlinkOrderSource implements SourceFunction<Order> {

    private boolean isRunning = true;
    private int index = 1;
    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        //每2s 产生一个订单，产生100条
        while (isRunning) {
            if (index < 100) {
                long shopId = (int)(Math.random()*10)+1;//商家id
                long userId = (int)(Math.random()*1000)+1;//用户id
                long GoodsId = (int)(Math.random()*100)+1;//商品id
                long categoryId = (int)(Math.random()*10)+1;//分类id
                long price = (long) (Math.random()*100)+1;//价格
                Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
                Order order = new Order(shopId, userId, GoodsId , categoryId, BigDecimal.valueOf(price), timestamp);
                ctx.collect(order);
                index++;
                Thread.sleep(2000);
            }  else {
                isRunning = false;
            }
        }
    }
    @Override
    public void cancel() {

    }
}
