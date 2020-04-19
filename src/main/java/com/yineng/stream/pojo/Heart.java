package com.yineng.stream.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class Heart {
    public String project;//项目
    public int classId;//直播id
    public String userId;//用户id
    public Timestamp time;//事件时间戳
//    public long time;//事件时间戳

    @Override
    public String toString() {
        return "HeartLog{" +
                "project=" + project +
                ", userId=" + userId +
                ", classId=" + classId +
                ", time=" + time +
                '}';
    }
}
