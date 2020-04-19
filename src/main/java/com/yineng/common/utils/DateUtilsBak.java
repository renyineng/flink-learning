package com.yineng.common.utils;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * simpleDateFormt.format 将Date 转为string
 * .parse 将date转为string
 */
public class DateUtilsBak {
    final static String defaultFormat = "yyyy-mm-dd hh:mm:ss";
    //时间格式转为时间戳
    public static long dateToTimeStamp(String date){
        return dateToTimeStamp(date, defaultFormat);
    }
    //时间格式转为时间戳
    public static long dateToTimeStamp(String date, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.parse(date, new ParsePosition(0)).getTime()/1000;

    }

    public static String timeStampToDate(Long timestamp){
        return timeStampToDate(timestamp, defaultFormat);

    }
    //时间戳转时间
    public static String timeStampToDate(Long timestamp, String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
//        String dateStr = simpleDateFormat.format(new Date(timestamp));
//        return simpleDateFormat.parse(dateStr, new ParsePosition(0));
        return simpleDateFormat.format(new Date(timestamp));

    }
}
