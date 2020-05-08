package com.yineng.stream.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * 每天每个商家每个分类 每个商品的销售情况
 */
@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class OrderAccumulator {
    private long shopId;//商家id
    private long categoryId;//分类id
    private long goodsId;//商品id
    long count;//订单数
    private int userCount ;//购买人数
    private BigDecimal amount = BigDecimal.valueOf(0);//订单金额
    private Timestamp startWindow;//窗口开始时间
    private Timestamp endWindow;//窗口结束时间

    @Override
    public String toString() {
        return "OrderAccumulator{" +
                "shopId=" + shopId +
                ", categoryId=" + categoryId +
                ", goodsId=" + goodsId +
                ", count=" + count +
                ", userCount=" + userCount +
                ", amount=" + amount +
                ", startWindow=" + startWindow +
                ", endWindow=" + endWindow +
                '}';
    }
}
