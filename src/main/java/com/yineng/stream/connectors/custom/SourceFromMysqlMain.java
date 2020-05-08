package com.yineng.stream.connectors.custom;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;

/**
 * 从mysql读取数据，读取技术完成后会退出，只能设置单个并行度 或者继承自RichSourceFunction 否则会多次读取
 */
@Slf4j
public class SourceFromMysqlMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<JSONObject> source = env.addSource(new ReadFromMysql()).setParallelism(1);//这块可以实例化 静态类是因为省略了SourceFromMysql 完整写法为new ReadFromMysql.ReadFromMysql()
        source.print();
        env.execute("diy source");
    }
//    public static class ReadFromMysql extends RichSourceFunction<JSONObject> {
    public static class ReadFromMysql extends RichParallelSourceFunction<JSONObject> {

        PreparedStatement ps;
        private Connection connection;
        private String driver="com.mysql.jdbc.Driver";
        private String url="jdbc:mysql://39.105.34.168:3306/abc?useUnicode=true&characterEncoding=UTF-8";
        private String user="dev";
        private String password="dev123456";

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Connection con = null;
            try {
                Class.forName(driver);
                //注意，这里替换成你自己的mysql 数据库路径和用户名、密码
                connection = DriverManager.getConnection(url, user, password);
            } catch (Exception e) {
                System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
            }
            String sql = "select * from test;";
            ps = this.connection.prepareStatement(sql);
        }

        /**
         * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) { //关闭连接和释放资源
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * DataStream 调用一次 run() 方法用来获取数据
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<JSONObject> ctx) throws Exception {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", resultSet.getInt("id"));
                jsonObject.put("name", resultSet.getString("name"));
                jsonObject.put("age", resultSet.getInt("age"));
                ctx.collect(jsonObject);
            }
        }
        @Override
        public void cancel() {

        }
    }
}
