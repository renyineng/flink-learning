package com.yineng.stream.keyby;

import com.alibaba.fastjson.JSONObject;
import com.yineng.common.utils.ExecutionEnvUtil;
import com.yineng.stream.pojo.HeartLog;
import com.yineng.stream.pojo.LiveOnlineUserStat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunction {
    public static void main(String[] args) throws Exception {
        // 初始化参数
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        // 创建env环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        DataStreamSource<String> source1 = KafkaSource.builder(parameterTool).readSource(env);
//        source1.printToErr();
        String sourcePath = parameterTool.get("source_path", "");
        DataStreamSource<String> source = env.readTextFile(sourcePath);
        SingleOutputStreamOperator<HeartLog> map = source.map(s -> JSONObject.parseObject(s, HeartLog.class));
        SingleOutputStreamOperator<LiveOnlineUserStat> heartStatStream = map.keyBy("appKey")
                .process(new KeyedProcessFunction<Tuple, HeartLog, LiveOnlineUserStat>() {

                    /**
                     * 处理分组内每一个元素，一般配合定时器或者状态计算 ValueState等
                     */
                    @Override
                    public void processElement(HeartLog heartLog, Context context, Collector<LiveOnlineUserStat> collector) throws Exception {
                        System.err.println(heartLog+"heartLogheartLog");
                        System.err.println(context.getCurrentKey()+"getCurrentKey");

                    }
                });
        source.print();
        heartStatStream.printToErr();
        env.execute("keyby learn");
    }
}
