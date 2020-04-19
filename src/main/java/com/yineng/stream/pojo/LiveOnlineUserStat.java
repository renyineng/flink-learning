package com.yineng.stream.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.Timestamp;

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class LiveOnlineUserStat {
    private String appKey;//机构
    private int classId;
    private long userCount;//用户数
    private long totalCount;//2分钟内总流量
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Timestamp windowStartTime;//窗口开始时间
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private Timestamp windowEndTime;//窗口结束时间
    //    @JSONField()
    private Date statDate;//日期



    @Override
    public String toString() {
        return "LiveOnlineUserStat{" +
                "appKey=" + appKey +
                ", classId=" + classId +
                ", statDate=" + statDate +
                ", windowStartTime=" + windowStartTime +
                ", windowEndTime=" + windowEndTime +
                ", userCount=" + userCount +
                ", totalCount=" + totalCount +
                '}';
    }
}
