package com.yineng.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadHBase {
    public static void main(String[] args) throws IOException {
        // 新建一个Configuration
        Configuration conf = HBaseConfiguration.create();
// 集群的连接地址，在控制台页面的数据库连接界面获得(注意公网地址和VPC内网地址)
        ParameterTool properties = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties"));
        conf.set("hbase.zookeeper.quorum", properties.get("hbase.zookeeper.quorum"));
// 设置用户名密码，默认root:root，可根据实际情况调整
//        conf.set("hbase.client.username", "root");
//        conf.set("hbase.client.password", "root");
        String taskId = "2";
        String rowKey = "2143_course";
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("test");
        Table table = connection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
//        for(KeyValue kv:result.getMap()) {
//
//        }
        String cf = "cf";
        Put put = new Put(Bytes.toBytes(rowKey));
//        result.getMap();
//        put.addColumn(Bytes.toBytes(cf), "task_ids".getBytes(), "".getBytes());
//        table.put(put);
        //字段存在
        if (result.containsNonEmptyColumn(cf.getBytes(), "task_ids".getBytes())) {
            String taskIdStr = new String(result.getValue(cf.getBytes(), "task_ids".getBytes()));
            List<String> taskIds = new ArrayList(Arrays.asList(taskIdStr.split(",")));
            System.err.println(taskIds+"taskIdstaskIds");
            //值不存在
            if (!taskIds.contains(taskId)) {
                System.err.println("不存在 要追加");
                taskIds.add(taskId);
                System.err.println(taskIds+"taskIds");
                put.addColumn(Bytes.toBytes(cf), "task_ids".getBytes(), String.join(",", taskIds).getBytes());
                table.put(put);
            } else {
                System.err.println("值存在");
            }
        } else {
            System.err.println("字段不存在");

            put.addColumn(Bytes.toBytes(cf), "task_ids".getBytes(), taskId.getBytes());
            table.put(put);

        }
    }
}
