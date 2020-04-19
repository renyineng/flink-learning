package com.yineng.stream.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class HeartLog {
    public String appKey;//机构
    public String psId;//用户唯一标识
    public int classId;
    public String userId;//用户id
    public long time;//事件时间戳

    @Override
    public String toString() {
        return "HeartLog{" +
                "appKey=" + appKey +
                ", psId=" + psId +
                ", userId=" + userId +
                ", classId=" + classId +
                ", time=" + time +
                '}';
    }
}
