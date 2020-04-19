package com.yineng.example.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Demo {
//    private static Cache<String, Timestamp> cache;
    private static transient volatile Cache<String, Date> cache;//用户id-project=>支付时间
    public static void main(String[] args) throws ExecutionException {
        cache = CacheBuilder.newBuilder().maximumSize(2000).expireAfterWrite(1, TimeUnit.DAYS).build();
//        Timestamp payTime = null;
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");// a为am/pm的标记
        Date date = new Date();// 获取当前时间

        cache.put("project_1", date);
//        Date value = cache.get("project_1");
        System.err.println(date+"valuevaluevalue");


    }
}
