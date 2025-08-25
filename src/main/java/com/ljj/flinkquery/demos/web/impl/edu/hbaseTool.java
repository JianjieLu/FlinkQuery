package com.ljj.flinkquery.demos.web.impl.edu;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import com.ljj.flinkquery.demos.entity.Location;

import static java.lang.Math.abs;

public class hbaseTool {

    public static String convertToHBaseTableName (long timestamp){
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH");

        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);

        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化为字符串
        String dateTimeStr = dateTime.format(formatter);
        return "JTSTCar_"+dateTimeStr;
    }
        public static String convertToCongestionTableName (long timestamp,int direction){
        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);

        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 格式化为字符串
        String dateTimeStr = dateTime.format(formatter);
        return "congestion"+"_"+dateTimeStr+"_"+direction;
    }
                //latitude,longitude,哪个匝道
    public static Pair<Location, Integer> UseLLGetSK(double Latitude, double Longitude, List<Location> roadDataList) throws IOException {
        // 精度、纬度 差值  第几个
        List<Pair<Pair<Location, Double>, Integer>> targets = new ArrayList<>();
        int i = 0;
        int j = 0;
        Location d = null;
        // 取出经度差值最小的十条数据
        for (Location l : roadDataList) {
            double diff = abs(l.getLatitude() - Latitude);
            if (i < 10) {
                targets.add(new Pair<>(new Pair<>(l, diff), i));
                i++;
            }else {
                // 只有当新的数据比当前最大的差值更小时，才替换并保持排序
                if (diff < targets.get(9).getKey().getValue()) {
                    targets.set(9, new Pair<>(new Pair<>(l, diff), i));
                    targets.sort(Comparator.comparing(p -> p.getKey().getValue())); // 重新排序
                }
                i++;
            }
        }
        double minDifference = 300;
        for (i = 0; i < 10; i++) {
            // 计算总差值
            double temp = targets.get(i).getKey().getValue() + abs(Longitude - targets.get(i).getKey().getKey().getLongitude());
            if (temp < minDifference) {
                d = targets.get(i).getKey().getKey();
                j=targets.get(i).getValue();
                minDifference = temp;
            }
        }
        if(abs(d.getLatitude()-Latitude) < 0.005&&abs(d.getLongitude()-Longitude) < 0.005){
        return new Pair<>(d, j);

        }else return null;
    }
       public static List<Location> readJsonFile(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(filePath);
        return objectMapper.readValue(file, objectMapper.getTypeFactory().constructCollectionType(List.class, Location.class));
    }
}
