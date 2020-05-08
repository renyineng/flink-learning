package com.yineng.stream.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private long shopId;//商家Id
    private long userId;//用户id
    private long goodsId;//商品id
    private long categoryId;//分类id
    private BigDecimal amount;//金额
//    private BigDecimal amount1;//金额
    private Timestamp createdAt;//事件时间
//    public long time;//事件时间戳

    @Override
    public String toString() {
        return "Order{" +
                "shopId=" + shopId +
                ", userId=" + userId +
                ", goodsId=" + goodsId +
                ", categoryId=" + categoryId +
                ", amount=" + amount +
                ", createdAt=" + createdAt +
                '}';
    }
}
