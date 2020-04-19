package com.yineng.common.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import static com.yineng.common.constants.PropertiesConstants.*;

public class ExecutionEnvUtil {
    public static ParameterTool createParameterTool(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return ParameterTool.fromPropertiesFile(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_CONFIG_NAME))
                .mergeWith(parameterTool)
                .mergeWith(ParameterTool.fromSystemProperties());
    }
    public static final ParameterTool PARAMETER_TOOL = createParameterTool();


    public static ParameterTool createParameterTool() {
        try {

            return ParameterTool.fromPropertiesFile(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_CONFIG_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();

    }
    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000));   //设置重启的策略
        String dir = parameterTool.get(STREAM_CHECKPOINT_DIR);
        String streamCheckPointType = parameterTool.get(STREAM_CHECKPOINT_TYPE);
        System.err.println(streamCheckPointType+"streamCheckPointType");
        env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 10000));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
//        设置 checkpoint 保留策略,取消程序时，保留 checkpoint 状态文件 */
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置操作检查点的超时时间
        config.setCheckpointTimeout(15000);
        StateBackend stateBackend = new FsStateBackend(dir);//设置checkpoint 检查点保存方式和路径
        env.setStateBackend(stateBackend);
        //本地开发
//        if ("memory".equals(streamCheckPointType)) {
//            //state 存放在内存中，默认是 5M
//            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 10);
//
//            env.setStateBackend(stateBackend);
//        } else if("fs".equals(streamCheckPointType)) {
////                    String dir = "file:///Users/renyineng/wwwroot/flink/checkpoint";
//            StateBackend stateBackend = new FsStateBackend(dir);//设置checkpoint 检查点保存方式和路径
//            env.setStateBackend(stateBackend);
//        } else {
//            StateBackend stateBackend = new RocksDBStateBackend(dir);
//            env.setStateBackend(stateBackend);
//        }

        return env;
    }
}
