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

import com.alibaba.fastjson.JSONObject;
import com.yineng.stream.pojo.HeartLog;
import com.yineng.stream.pojo.LiveOnlineUserStat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class EventTime {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置时间属性为ProcessingTime
		DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9000);
		source.print();
//		new JSONOB
		SingleOutputStreamOperator<LiveOnlineUserStat> liveOnlineStream = source
				.filter(s->{
					HeartLog heartLog = JSONObject.parseObject(s, HeartLog.class);
					if (heartLog == null) {
						return false;
					}
					return true;
				})
				.map(s -> {
					HeartLog heartLog = JSONObject.parseObject(s, HeartLog.class);
					heartLog.setTime(heartLog.getTime()*1000);
					return heartLog;
				})
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<HeartLog>() {
					private long currentTimestamp = Long.MIN_VALUE;
//					private long currentTimestamp = 0L;

					private final long maxTimeLag = 5000;//允许迟到5s
					@Nullable
					@Override
					//获取当前水位线 watermark
					public Watermark getCurrentWatermark() {
//						System.err.println("wall clock is " + System.currentTimeMillis() + " new watermark "
//								+ (currentTimestamp - maxTimeLag));
						return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
					}
					//从数据流提取eventTime
//					Flink在指定WaterMark时，先调用extractTimestamp方法，再调用getCurrentWatermark方法， 所以打印信息中的WaterMark为上一条数据的WaterMark，并非当前的WaterMark
					@Override
					public long extractTimestamp(HeartLog heartLog, long previousElementTimestamp) {
						long timestamp = heartLog.getTime();
						System.err.println(
								"get timestamp is " + timestamp + " currentMaxTimestamp " + currentTimestamp);
						currentTimestamp = Math.max(timestamp, currentTimestamp);
						return timestamp;
					}
				})
				.keyBy("appKey")
				.timeWindow(Time.seconds(10))
				//输入  输出  key
				.process(new HearLogProcessWindowFunction());
		liveOnlineStream.printToErr();
		env.execute("flink Streaming watermark ");
	}

	public static class HearLogProcessWindowFunction extends ProcessWindowFunction<HeartLog, LiveOnlineUserStat, Tuple, TimeWindow> {
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
