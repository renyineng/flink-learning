package com.yineng.example;

import com.alibaba.fastjson.JSONObject;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.now();
        System.err.println(now);
        System.err.println(now.plusSeconds(1));
        System.err.println("====");
        System.err.println(Math.random());
        System.err.println(Math.random()*10);
        System.err.println((int)(Math.random()*10));
        String str = "{\"52510\":{\"score\":\"1.80100\",\"times\":2,\"qid\":52510,\"status\":1,\"responseid\":\"20180606120954\",\"hash_id\":\"e8733a0813968dbe6ccf5ebfa56faf7d\"}}";
        JSONObject jsonObject = JSONObject.parseObject(str);
        System.err.println(jsonObject.keySet());
        Set set = new HashSet();
        set.add(1);
        set.add(1);
        set.add(2);
        System.err.println(set+"set");
        for (String key:jsonObject.keySet()) {
            JSONObject jsonObject1 = jsonObject.getJSONObject(key);
            System.err.println(jsonObject1+"jsonObject1jsonObject1");
            System.err.println(jsonObject1.getBigDecimal("score")+"score");
        }
    }
}
