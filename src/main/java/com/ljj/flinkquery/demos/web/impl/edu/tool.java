package com.ljj.flinkquery.demos.web.impl.edu;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class tool {
        public static Long toDateTimeLong(String timeStr){
        // 创建格式化器
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
        // 解析为LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse(timeStr, formatter);
        // 应用上海时区
        ZonedDateTime shanghaiTime = localDateTime.atZone(ZoneId.of("Asia/Shanghai"));
        // 转换为时间戳（毫秒）
        return shanghaiTime.toInstant().toEpochMilli();
    }
}
