package com.ljj.flinkquery.demos.web.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import com.ljj.flinkquery.demos.entity.VehicleSeg;
import com.ljj.flinkquery.demos.entity.newFive.firstResult;
import com.ljj.flinkquery.demos.entity.newFive.secondResult;
import com.ljj.flinkquery.demos.entity.newFive.totalResult;
import com.ljj.flinkquery.demos.web.service.BasicService;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps;
import javafx.util.Pair;
import org.apache.hadoop.hbase.CellUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getDayOfYear;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getTodayTotal;

@Service
public class BasicServiceImpl implements BasicService {
 @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Autowired
    private RedisTemplate<String, String> redisTemplate1;
 @Override
    public Map<Integer, Integer> getVehicleTypesByDuration(Long startTime, Long endTime) {
        return totalOps.getVehicleTypesByDuration(startTime, endTime);
    }
    @Override
    public String getByStake(Long startTime, Long endTime, String startMileage, String endMileage) {
        return "";
    }

    @Override
    public List<String> getRowKeysByQualifier(String tableName, String cf, String quali) throws IOException {
        return totalOps.getRowKeysByQualifier( tableName,  cf,  quali);
    }
    @Override
    public List<totalOps.VehicleData> getAllVehicleData(
        String tableName,
        List<String> qualifiers
    ) throws IOException {
     return totalOps.getAllVehicleData(tableName,qualifiers);
    }
     @Override
    public boolean deleteTable(String tableName) throws IOException {
      return totalOps.deleteTable(tableName);
     }

@Override
public List<totalOps.TrajData> getTrajInTimeRange(Long startTime, Long endTime) throws IOException {
    return totalOps.getTrajInTimeRange(startTime, endTime);
}

    @Override
    public List<totalOps.VehicleData> getVehicleDataInTimeRange(Long startTime, Long endTime, List<String> qualifier) throws IOException {
        return totalOps.getVehicleDataInTimeRange(startTime,endTime,qualifier);
    }

    @Override
    public Map<String, Map<Integer, Integer>> getHourlyTrafficStatistics(long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {

        return totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp);
    }

    @Override
    public totalResult getHolyTotal(Long timestamp) throws IOException, ExecutionException, InterruptedException {
        Set<String> keys = redisTemplate.keys("v2*");
        int sum = 0;
        int upSum = 0;
        int downSum = 0;
        int busUp=0;
        int busDown=0;
        int trackUp=0;
        int trackDown=0;




        for(String redisKey:keys) {
            String jsonData = redisTemplate.opsForValue().get(redisKey);
            if (jsonData != null && !jsonData.isEmpty()) {
                if (jsonData.startsWith("\"") && jsonData.endsWith("\"")) {
                    jsonData = jsonData.substring(1, jsonData.length() - 1).replace("\\\"", "\"");
                }
                String o = "";
                List<VehicleSeg> l = new ArrayList<>();
                try {
                    JSONArray objects = JSON.parseArray(jsonData);
                    for (Object object : objects) {
                        o = object.toString();
                        l.add(JSON.parseObject(object.toString(), VehicleSeg.class));
                    }
                } catch (JSONException e) {
                    // 记录错误日志，包括键和无效的JSON数据
                    System.err.println("JSON解析失败，键: " + redisKey);
                    System.err.println("无效的JSON数据: " + o);
                    e.printStackTrace();
                }

                for (VehicleSeg v : l) {
                    sum++;
                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    boolean b = vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
                    System.out.println("vt:"+vt+"vet："+vet);
                    if (v.getDirection() == 1) {
                        upSum++;
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) busUp++;
                        else if (b)
                            trackUp++;

                    } else {
                        downSum++;
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) busDown++;
                        else if (b) trackDown++;
                        else {
                            if (vet >= 1 && vet <= 4) busDown++;
                            else if (vet >= 11 && vet <= 16) trackDown++;
                            else {
                                sum--;
                                downSum--;
                            }
                        }
                    }
                }
            }
        }
        System.out.println("upSum:"+upSum+"downSum:"+downSum+"busUp:"+busUp+"busDown:"+busDown);
        secondResult second = new secondResult(upSum,downSum,busUp,trackUp,busDown,trackDown,
                Math.round((((double) busUp /upSum) * 100.0)) / 100.0,
                Math.round((((double) trackUp /upSum) * 100.0)) / 100.0,
                Math.round((((double) busDown /downSum) * 100.0)) / 100.0,
                Math.round((((double) trackDown /downSum) * 100.0)) / 100.0);

            int asd=getDayOfYear(timestamp);
            int[]ad= totalOps.getYearToDateTraffic(timestamp);
            //数量，时间
        Pair<Integer,Integer> np=getTodayTotal(timestamp);
        //今日车流量  年日均交通量  车流量走势
        firstResult ff=new firstResult();
        ff.setDownCrowdLength(np.getKey());
//        return new totalResult(totalOps.getNearestMinuteCongestionStats(timestamp),second,getTodayTotal(timestamp),Math.round((((double) ad[0]/asd) * 100.0)) / 100.0,Math.round((((double) busUp /upSum) * 100.0)) / 100.0,totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp));
        return new totalResult(ff,second,np.getValue(),Math.round((((double) ad[0]/asd) * 100.0)) / 100.0,Math.round((((double) ad[1]/asd) * 100.0)) / 100.0,totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp));
    }

    public int getDailyVehicleCount() {
        // 从 Redis 中获取当日车辆计数
        String countStr = redisTemplate.opsForValue().get("daily_vehicle_count");

        // 如果键存在，解析为整数；否则返回0
        if (countStr != null && !countStr.isEmpty()) {
            try {
                return Integer.parseInt(countStr);
            } catch (NumberFormatException e) {
                // 记录错误日志
                System.err.println("Redis中daily_vehicle_count的值不是有效的整数: " + countStr);
                e.printStackTrace();
                return 0;
            }
        }
        return 0; // 键不存在或值为空时返回0
    }


    @Override
    public double[] test1(Long timestamp) throws IOException {
         int asd = getDayOfYear(timestamp);
    int[] ad = totalOps.getYearToDateTraffic(timestamp);
    double[] result = {Math.round((((double) ad[0] / asd) * 100.0)) / 100.0,Math.round((((double) ad[1] / asd) * 100.0)) / 100.0};
    return result;
    }

    @Override
    public secondResult test2(Long timestamp) {
         Set<String> keys = redisTemplate.keys("v2*");
        int sum = 0;
        int upSum = 0;
        int downSum = 0;
        int busUp=0;
        int busDown=0;
        int trackUp=0;
        int trackDown=0;




        for(String redisKey:keys) {
            String jsonData = redisTemplate.opsForValue().get(redisKey);
            if (jsonData != null && !jsonData.isEmpty()) {
                if (jsonData.startsWith("\"") && jsonData.endsWith("\"")) {
                    jsonData = jsonData.substring(1, jsonData.length() - 1).replace("\\\"", "\"");
                }
                String o = "";
                List<VehicleSeg> l = new ArrayList<>();
                try {
                    JSONArray objects = JSON.parseArray(jsonData);
                    for (Object object : objects) {
                        o = object.toString();
                        l.add(JSON.parseObject(object.toString(), VehicleSeg.class));
                    }
                } catch (JSONException e) {
                    // 记录错误日志，包括键和无效的JSON数据
                    System.err.println("JSON解析失败，键: " + redisKey);
                    System.err.println("无效的JSON数据: " + o);
                    e.printStackTrace();
                }

                for (VehicleSeg v : l) {
                    sum++;
                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    boolean b = vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
                    if (v.getDirection() == 1) {
                        upSum++;
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) busUp++;
                        else if (b)
                            trackUp++;

                    } else {
                        downSum++;
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) busDown++;
                        else if (b) trackDown++;
                        else {
                            if (vet >= 1 && vet <= 4) busDown++;
                            else if (vet >= 11 && vet <= 16) trackDown++;
                            else {
                                sum--;
                                downSum--;
                            }
                        }
                    }
                }
            }
        }
        secondResult second = new secondResult(upSum,downSum,busUp,trackUp,busDown,trackDown,
                Math.round((((double) busUp /upSum) * 100.0)) / 100.0,
                Math.round((((double) trackUp /upSum) * 100.0)) / 100.0,
                Math.round((((double) busDown /downSum) * 100.0)) / 100.0,
                Math.round((((double) trackDown /downSum) * 100.0)) / 100.0);
        return second;
    }

    @Override
    public Map<String, Map<Integer, Integer>> test3(Long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        return totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp);
    }

    // 统计内部类
private static class Statistics {
    int sum = 0;
    int upSum = 0;
    int downSum = 0;
    int busUp = 0;
    int trackUp = 0;
    int busDown = 0;
    int trackDown = 0;
}
}
