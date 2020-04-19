package com.yineng.stream.connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;
import java.util.Collections;

public class SocketSourceMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从socket中读取数据源  需要先执行 nc -l 9000 ,否则会拒接链接
        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split("\\W+");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        })
            .keyBy(0)
            .sum(1)
            .printToErr();

        //如果使用lambda 则需要如下写法 增加returns 因为
//        text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
//            String[] words = value.split("\\W+");
//            for (String word : words) {
//                collector.collect(Tuple2.of(word, 1));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        env.execute("predefined source");
    }
}
