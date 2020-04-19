package com.yineng.stream.connectors;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.Arrays;
import java.util.Collections;

public class FileSourceMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //按照 TextInputFormat 格式读取文本文件或者目录，并将其内容以字符串的形式返回
        String filePath = "src/main/resources/file/binlog.txt";
        DataStreamSource<String> source = env.readTextFile(filePath);

        source.printToErr();

        //按照指定格式周期性的读取文件
        String filePath1 = "src/main/resources/file";
        final TextInputFormat format = new TextInputFormat(new Path(filePath1));
        //设置过滤规则
        GlobFilePathFilter filesFilter = new GlobFilePathFilter(
                Collections.singletonList("**"),
                Arrays.asList("**/keyby.txt")
        );
        format.setFilesFilter(filesFilter);
//        DataStreamSource<String> stringDataStreamSource = env.readFile(format, filePath1, FileProcessingMode.PROCESS_CONTINUOUSLY, 1L);
//        stringDataStreamSource.print();
        //sink 写入文件
        source.map(s-> JSONObject.parseObject(s)).writeAsText("./aa.txt", FileSystem.WriteMode.OVERWRITE);
//        source.writeToSocket("127.0.0.1", 9000, new SimpleStringSchema());
        //sink 写入excel
        //写入socket
//        http://www.justdojava.com/2019/12/01/flink_learn_window/

//        https://juejin.im/post/5dd2661af265da0be53ea1e4
        env.execute("predefined source");
    }
}
