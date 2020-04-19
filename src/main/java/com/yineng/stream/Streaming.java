/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yineng.stream;

import com.yineng.common.utils.ExecutionEnvUtil;
import com.yineng.common.utils.KafkaSource;
import com.yineng.common.utils.MySqlUtil;
import com.yineng.stream.pojo.HeartLog;
import com.yineng.stream.pojo.LiveOnlineUserStat;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.alibaba.fastjson.JSONObject;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;

public class Streaming {
	private static final OutputTag<HeartLog> outputTag = new OutputTag<HeartLog>("output") {
	};
	public static void main(String[] args) throws Exception {
		// 初始化参数
		ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

		// 创建env环境
		StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置时间属性为ProcessingTime
//		DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9000);
		DataStreamSource<String> source = KafkaSource.builder(parameterTool).readSource(env);
		env.setParallelism(1);
//		source.print();
		SingleOutputStreamOperator<LiveOnlineUserStat> liveOnlineStream = source
//				.filter(s->{
//					HeartLog heartLog = JSONObject.parseObject(s, HeartLog.class);
//					if (heartLog == null) {
//						return false;
//					}
//					return true;
//				})
				.map(s -> {
					HeartLog heartLog = JSONObject.parseObject(s, HeartLog.class);
					heartLog.setTime(heartLog.getTime()*1000);
//					System.err.println(heartLog+"hearlog");
					return heartLog;
				})
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<HeartLog>() {
//					private long currentTimestamp = Long.MIN_VALUE;

					private final long maxTimeLag = 5000;//允许迟到5s
					private long currentMaxTimestamp = 0L;
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


					@Nullable
					@Override
					public Watermark getCurrentWatermark() {
						return new Watermark(currentMaxTimestamp - maxTimeLag);
					}
					@Override
					public long extractTimestamp(HeartLog heartLog, long previousElementTimestamp) {


						long timestamp = heartLog.getTime();
//						System.err.println("key:"+timestamp+",eventtime:["+timestamp+"|"+sdf.format(timestamp)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
//								sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
						currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
						return timestamp;
					}
				})
				.keyBy("appKey", "classId")
//				.keyBy("appKey", "classId")
				.timeWindow(Time.seconds(30))
//				.allowedLateness(Time.seconds(2))
//				.sideOutputLateData(outputTag)
				.process(new HearLogProcessWindowFunction());
//		liveOnlineStream.print();
		liveOnlineStream.printToErr();
		SingleOutputStreamOperator<Row> orderLogsDataRows = liveOnlineStream.map(s -> {
			Row row = new Row(6);
			row.setField(0, s.getAppKey());
			row.setField(1, s.getClassId());
			row.setField(2, s.getStatDate());
			row.setField(3, s.getWindowStartTime());
			row.setField(4, s.getUserCount());
			row.setField(5, s.getTotalCount());


			return row;
		});

		int[] fields = {
				Types.VARCHAR,
				Types.INTEGER,
				Types.DATE,
				Types.TIMESTAMP,
				Types.BIGINT,
				Types.BIGINT,

		};

		MySqlUtil mySqlUtil = MySqlUtil.builder().setSqlField(fields);

		//写入或更新订单商品表
		String query = "replace into dwd_live_online_users(app_key, class_id, stat_date, stat_time, user_count, total_count) values(?, ?, ?, ?, ?, ?);";

		orderLogsDataRows.writeUsingOutputFormat(mySqlUtil.setQuery(query).finish());

//		DataStream<HeartLog> sideOutput = liveOnlineStream.getSideOutput(outputTag);
//		sideOutput.print();
		env.execute("Flink Streaming live heart");
	}

	public static class HearLogProcessWindowFunction extends ProcessWindowFunction<HeartLog, LiveOnlineUserStat, Tuple, TimeWindow> {
		private ValueState<LiveOnlineUserStat> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ValueStateDescriptor<LiveOnlineUserStat> myState = new ValueStateDescriptor<>("usePlanState", LiveOnlineUserStat.class);
			state = getRuntimeContext().getState(myState);
		}

		@Override
		public void process(Tuple key, Context context, Iterable<HeartLog> iterable, Collector<LiveOnlineUserStat> out) throws Exception {
			long count = 0;
			List<String> userList =new ArrayList<>();
			for (HeartLog heartlog : iterable) {
				boolean isContains =userList.contains(heartlog.psId);
				if(!isContains){
					userList.add(heartlog.psId);
				}
				count++;
			}
			long start = context.window().getStart();
			LiveOnlineUserStat liveOnlineUserStat = new LiveOnlineUserStat();
			liveOnlineUserStat.setAppKey(key.getField(0));
			liveOnlineUserStat.setClassId(key.getField(1));
			liveOnlineUserStat.setUserCount(userList.size());
			liveOnlineUserStat.setTotalCount(count);
//			liveOnlineUserStat.setWindowStartTime(Timestamp.valueOf(sdf.format(new Date(context.window().getStart()))));
			liveOnlineUserStat.setWindowStartTime(new Timestamp(start));
			liveOnlineUserStat.setStatDate(new java.sql.Date(start));
			liveOnlineUserStat.setWindowEndTime(new Timestamp(context.window().getEnd()));
//			liveOnlineUserStat.appKey = key;
			out.collect(liveOnlineUserStat);

		}
	}
}
