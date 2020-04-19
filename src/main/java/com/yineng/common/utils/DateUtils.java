package com.yineng.common.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * simpleDateFormt.format 将Date 转为string
 * .parse 将date转为string
 */
public class DateUtils {
    private static String defaultPattern = "yyyy-MM-dd HH:mm:ss";//默认规则
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern);

    //自定义解析格式
    public static void setDefaultFormat(String pattern) {
        DateUtils.defaultPattern = pattern;
        DateUtils.dateTimeFormatter = DateTimeFormatter.ofPattern(defaultPattern);
    }

    /**
     * 字符串 转为时间格式
     * @param dateStr
     * @return
     */
    public static LocalDateTime parse(String dateStr) {
        return LocalDateTime.parse(dateStr, dateTimeFormatter);
    }

}
