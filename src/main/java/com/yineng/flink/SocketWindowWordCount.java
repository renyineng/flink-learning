package com.yineng.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // 自动获取当前的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从socket中读取数据源  需要先执行 nc -l 9000
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

//        String dir = "file:///Users/renyineng/wwwroot/flink/checkpoint";
//        StateBackend stateBackend = new FsStateBackend(dir);//设置checkpoint 检查点保存方式和路径
//
//        env.enableCheckpointing(5000);//5s一次checkpoint
//        env.setStateBackend(stateBackend);
//
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//设置EXACTLY_ONCE 语义，保证数据只处理一次
//        CheckpointConfig config = env.getCheckpointConfig();
////        设置 checkpoint 保留策略,取消程序时，保留 checkpoint 状态文件 */
//        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置操作检查点的超时时间
//        config.setCheckpointTimeout(15000);
        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        //flatmap 算子
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")//按照单词分组
//                .timeWindow(Time.seconds(10), Time.seconds(3))
                .reduce(new ReduceFunction<WordWithCount>() {
                    //reduce阶段，a 和b 分别是上一条流和本次流，可以在这里进行更加丰富的聚合操作
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // sink 输出到控制台 设置1个并行度
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");//执行flink程序
    }
    // 输出的pojo类
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
