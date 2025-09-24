package com.ljj.flinkquery.demos.web.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.entity.watch.*;
import com.ljj.flinkquery.demos.web.impl.edu.hbaseTool;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.LocationOP;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOpsv3;
import com.ljj.flinkquery.demos.web.impl.edu.tools.HBaseTableScanner;
import com.ljj.flinkquery.demos.web.impl.edu.tools.HighwaySectionUtils;
import io.lettuce.core.ScriptOutputType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org. json. JSONObject;
import com.ljj.flinkquery.FlinkQueryApplication;
import com.ljj.flinkquery.demos.entity.newFive.firstResult;
import com.ljj.flinkquery.demos.entity.newFive.secondResult;
import com.ljj.flinkquery.demos.entity.newFive.totalResult;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOpsV1;
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
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.ljj.flinkquery.FlinkQueryApplication.VehicleCounter.getYearToDateTrafficMemory;
import static com.ljj.flinkquery.FlinkQueryApplication.getSau;
import static com.ljj.flinkquery.FlinkQueryApplication.getTodayTotalMemory;
import static com.ljj.flinkquery.demos.entity.data.Utils.convertFromTimestampMillis;
import static com.ljj.flinkquery.demos.entity.data.Utils.convertToTimestamp;
import static com.ljj.flinkquery.demos.web.impl.edu.querys.RampTrafficQuery.*;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getDayOfYear;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getNearestMinuteCongestionStats;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOpsV2.getUpDownChargerByDuration1;
import static com.ljj.flinkquery.demos.web.impl.edu.tools.HBaseTableScanner.generateScanPlan;
import static com.ljj.flinkquery.demos.web.impl.edu.tools.HighwaySectionUtils.*;
import static com.ljj.flinkquery.demos.web.impl.myTools.convert;
import static com.ljj.flinkquery.demos.web.service.HBaseServiceImpl.*;
import static java.lang.Math.abs;

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
        return totalOps.getRowKeysByQualifier(tableName, cf, quali);
    }

    @Override
    public List<totalOps.VehicleData> getAllVehicleData(
            String tableName,
            List<String> qualifiers
    ) throws IOException {
        return totalOps.getAllVehicleData(tableName, qualifiers);
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
        return totalOps.getVehicleDataInTimeRange(startTime, endTime, qualifier);
    }

    @Override
    public Map<String, Map<Integer, Map<Integer, Integer>>> getHourlyTrafficStatistics(long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {

        return totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp);
    }

    @Override
    public upDownResult getUpDownChargerByDuration(String stationId, String startTime, String endTime) {
        long st = convertToTimestamp(startTime);
        long et = convertToTimestamp(endTime);
        return new upDownResult(200, "查询成功", totalOpsV1.queryTrafficData(stationId, st, et), true);
    }
        // 批量查询方法
    @Override
    public List<upDownResult> getBatchUpDownCharger(List<String> stationIds, String beginTime, String endTime, int level) {
        List<upDownResult> l = new ArrayList<>();
        for (String stationId : stationIds) {
            l.add(getUpDownChargerByDuration1(stationId, beginTime, endTime, level));
        }
        return l;
    }

    @Override
    public totalResult getHolyTotal(Long timestamp) throws IOException, ExecutionException, InterruptedException {
        long st = System.currentTimeMillis();
        System.out.println("getHolyTotal.start: " + st);
        Set<String> keys = redisTemplate.keys("v2*");
        int sum = 0;
        int upSum = 0;
        int downSum = 0;
        int busUp = 0;
        int busDown = 0;
        int trackUp = 0;
        int trackDown = 0;


        for (String redisKey : keys) {
            String jsonData = redisTemplate1.opsForValue().get(redisKey);
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
                        else if (b) trackUp++;
                        else {
                            if (vet >= 1 && vet <= 4) busUp++;
                            else if (vet >= 11 && vet <= 16) trackUp++;
                            else {
                                sum--;
                                upSum--;
                            }
                        }
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
        System.out.println("upSum:" + upSum + "downSum:" + downSum + "busUp:" + busUp + "busDown:" + busDown);
        secondResult second = new secondResult(upSum, downSum, busUp, trackUp, busDown, trackDown,
                Math.round((((double) busUp / upSum) * 100.0)) / 100.0,
                Math.round((((double) trackUp / upSum) * 100.0)) / 100.0,
                Math.round((((double) busDown / downSum) * 100.0)) / 100.0,
                Math.round((((double) trackDown / downSum) * 100.0)) / 100.0);

        int asd = getDayOfYear(timestamp);
        int[] ad = getYearToDateTrafficMemory();
        //数量，时间
        Pair<Integer, Integer> np = getTodayTotalMemory(timestamp);
        //今日车流量  年日均交通量  车流量走势
        firstResult f = getNearestMinuteCongestionStats(timestamp);
        System.out.println("getHolyTotal.end-start: " + (System.currentTimeMillis() - st));
        Map<String, Map<Integer, Map<Integer, Integer>>> hourlyTrafficForDayAndPreviousDay = totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp);
//        return new totalResult(totalOps.getNearestMinuteCongestionStats(timestamp),second,getTodayTotal(timestamp),Math.round((((double) ad[0]/asd) * 100.0)) / 100.0,Math.round((((double) busUp /upSum) * 100.0)) / 100.0,totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp));
        System.out.println("上行：ad[0]"+ad[0]+"asd:"+asd+"下行:ad[1]"+ad[1]);
        return new totalResult(f, second, np.getKey(), Math.round((((double) ad[0] / asd) * 100.0)) / 100.0, Math.round((((double) ad[1] / asd) * 100.0)) / 100.0, hourlyTrafficForDayAndPreviousDay);
    }


    @Override
    public Set<String> getAllPlateNumbers() {
        return FlinkQueryApplication.getAllPlateNumbers();
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
        double[] result = {Math.round((((double) ad[0] / asd) * 100.0)) / 100.0, Math.round((((double) ad[1] / asd) * 100.0)) / 100.0};
        return result;
    }

    @Override
    public secondResult test2(Long timestamp) {
        Set<String> keys = redisTemplate.keys("v2*");
        int sum = 0;
        int upSum = 0;
        int downSum = 0;
        int busUp = 0;
        int busDown = 0;
        int trackUp = 0;
        int trackDown = 0;


        for (String redisKey : keys) {
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
                        else if (b) trackUp++;
                        else {
                            if (vet >= 1 && vet <= 4) busDown++;
                            else if (vet >= 11 && vet <= 16) trackDown++;
                            else {
                                sum--;
                                upSum--;
                            }
                        }
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
        secondResult second = new secondResult(upSum, downSum, busUp, trackUp, busDown, trackDown,
                Math.round((((double) busUp / upSum) * 100.0)) / 100.0,
                Math.round((((double) trackUp / upSum) * 100.0)) / 100.0,
                Math.round((((double) busDown / downSum) * 100.0)) / 100.0,
                Math.round((((double) trackDown / downSum) * 100.0)) / 100.0);
        return second;
    }

    @Override
    public Map<String, Map<Integer, Map<Integer, Integer>>> test3(Long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        return totalOps.getHourlyTrafficForDayAndPreviousDay(timestamp);
    }




    @Override
    public FlinkQueryApplication.trj getTrajectoryByPlateNo(String plateNo) throws IOException {
        return FlinkQueryApplication.getTrajectoryByPlateNo(plateNo);
    }



    @Override
    public sectionLosResult sectionLOS(String beginTime, String endTime1, String startStake, String endStake) throws IOException {
        long currentTime = System.currentTimeMillis();
        long timeSplit = (currentTime - 120000) / 10000 * 10000;
        long startTime = convertToTimestamp(beginTime);
        long endTime = convertToTimestamp(endTime1);
        System.out.println("startTime-currentTime:" + abs(startTime - currentTime));
        System.out.println("startTime：" + startTime);
        System.out.println("currentTime：" + currentTime);
        if (abs(startTime - currentTime) < 20000) {
            return sectionLOSRedis(beginTime, endTime1, startStake, endStake);
        }
        if (startTime > timeSplit) {
            return sectionLOSRedis(beginTime, endTime1, startStake, endStake);
        } else if (endTime > timeSplit & startTime < timeSplit) {
            return new sectionLosResult(200, "内存、数据库同时查询", sectionLOSRedis(beginTime, endTime1, startStake, endStake).getData(), true);
        } else if (endTime < timeSplit) {
            return sectionLOSHbase(beginTime, endTime1, startStake, endStake);
        }
        return new sectionLosResult(200, "起始时间必须大于终止时间", null, true);

    }


//    public sectionLosResult sectionLOSHbase(String beginTime, String endTime, String startStake, String endStake) throws IOException {
//        //        Set<String> keys = redisTemplate.keys("v*");
////        System.out.println(keys);
//        long t1 = System.currentTimeMillis();
//        //region Description
//        if (startStake != null) startStake = startStake.replace(" ", "+");
//        if (endStake != null) endStake = endStake.replace(" ", "+");
//        int shangxing1 = 0;
//        int xiaxing2 = 0;//下行车辆数
//        double shangxingSum = 0;
//        double xiaxingSum = 0;//下行平均速度总和
//        String startMi = "";
//        String endMi = "";//桩号（完整版）
//        int startM = 0;
//        int endM = 0;//桩号中的数字
//        int upkeche = 0;
//        int uphuoche = 0;
//        int upweihuaping = 0;
//        int upzhongxinghuoche = 0;
//        int downkeche = 0;
//        int downhuoche = 0;
//        int downweihuaping = 0;
//        int downzhongxinghuoche = 0;
//        int zupkeche = 0;
//        int zuphuoche = 0;
//        int zupweihuaping = 0;
//        int zupzhongxinghuoche = 0;
//
//        String zadaoInfo = "匝道查找信息：";
//        int zdeltam = 0;
//        int zalen = 0;
//        long startTime = convertToTimestamp(beginTime);
//        long edt = convertToTimestamp(endTime);
//        long st = startTime / 1000 * 1000;
//        long tt = edt / 1000 * 1000 + 1000;
//
//        String zaStartMil = "";
//        String zaEndMil = "";
//        TimeSpatialData kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0", "0");
//        TimeSpatialResult t2 = new TimeSpatialResult(200, "内存查找————无范围内数据,原因：桩号转换失败", kong, true);
//        TimeSpatialData tos = new TimeSpatialData();
//        boolean za = true;
//        boolean main = true;
//        stakeEnvents.StakeAssignment stakeAssign;
//        int index;
//        int index1;
//        String startMil;
//        String endMil;
//        double n = 0;
//        double sum = 0;
//        double zn = 0;
//        double zsum = 0;
//        Map<Long, VehicleSeg> m = new HashMap<>();
////Set<String> keys = redisTemplate.keys("v*");
////            System.out.println("keys: "+keys);
////        System.out.println(System.currentTimeMillis());
//
////            System.out.println("开始数据库查找，查找语句：http://100.65.38.139:8080/getByTimeSpatialWithID?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
//        shangxing1 = 0;
//        xiaxing2 = 0;//下行车辆数
//        shangxingSum = 0;
//        xiaxingSum = 0;//下行平均速度总和1746857652000
//        startMi = "";
//        endMi = "";//桩号（完整版）
//        startM = 0;
//        endM = 0;//桩号中的数字
//
//
//        zdeltam = 0;
//        zalen = 0;
//        st = startTime / 60000 * 60000;
//        tt = edt / 60000 * 60000 + 60000;
//        zaStartMil = "";
//        zaEndMil = "";
//        kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0", "0");
//        t2 = new TimeSpatialResult(200, "8001————无范围内数据,原因：桩号转换失败", kong, true);
//        tos = new TimeSpatialData();
//        za = true;
//        main = true;
//        zadaoInfo = "匝道查找信息：";
//
//        startMi = startStake;
//        endMi = endStake;
//        if (!Objects.equals(startStake, "") && !Objects.equals(endStake, "")) {
//
//
//            index = startMi.indexOf("+");
//            startMil = (index != -1) ? startMi.substring(0, index) : startMi;
//            index1 = endMi.indexOf("+");
//            endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;
//
//            n = 0;
//            sum = 0;
//            zn = 0;
//            zsum = 0;
//
//
//            List<sectionLos> slsList = new ArrayList<>();
//
//            //从起始时间到终止时间
//            startM = Integer.parseInt(startMil.substring(1));//前四个数字
//            endM = Integer.parseInt(endMil.substring(1)) + 1;
//            System.out.println("startMil.substring(1):" + startMil.substring(1) + "  endMil.substring(1):" + endMil.substring(1) + "  startM:" + startM + "  endM:" + endM);
//            if (startM > endM) {
//                int temp = endM;
//                endM = startM;
//                startM = temp;
//            }
//                Map<String, HBaseTableScanner.KeyRange> scanPlan = generateScanPlan(startTime, edt);
//        for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {}
//            for (long i = st; i < tt; i += 60000) {
//                for (int j = startM; j < endM; j++) {
//                    List<VehicleSeg> l = totalOps.getVeByRowkeys(hbaseTool.convertToHBaseTableName(i), i + "_K" + j + "_1", i + "_K" + j + "_2");
//                    for (VehicleSeg vs : l) {
//                        //判断是否有重复出
//                        if (m.get(vs.getCarId()) == null) {
//                            m.put(vs.getCarId(), vs);
//                        } else {
//                            vs.setPlateNo(vs.getPlateNo());
//                            vs.setDirection(vs.getDirection());
//                            m.put(vs.getCarId(), vs);
//                        }
//                    }
//                }
//            }
//
//
//            for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
//
//                VehicleSeg v = entry.getValue();
//                if (v != null) {
//                    n++;
//                    sum += v.getAverageSpeed();
//
//                    if (v.getDirection() == 1) {
//                        shangxing1++;
//                        System.out.println(shangxing1);
//                        shangxingSum += v.getAverageSpeed();
//                        Integer vt = v.getOriginalType();
//                        Integer vet = v.getVehicleType();
//                        if (vt != null) {
//                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
//                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                                uphuoche++;
//                            else if (vt == 8) {
//                                upweihuaping++;
//                                uphuoche++;
//                            }
//                        } else {
//                            if (vet >= 1 && vet <= 4) upkeche++;
//                            else if (vet >= 11 && vet <= 16) uphuoche++;
//                            else {
//                                n--;
//                                shangxing1--;
//                                shangxingSum -= v.getAverageSpeed();
//                            }
//                        }
//                        if (v.getSpecialFlag() != null) {
//                            String[] sd = v.getSpecialFlag().split(";");
//                            for (String s : sd)
//                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                    upzhongxinghuoche++;
//                        }
//
//                    } else if (v.getDirection() == 2) {
//                        xiaxing2++;
//                        xiaxingSum += v.getAverageSpeed();
//                        Integer vt = v.getOriginalType();
//                        Integer vet = v.getVehicleType();
//                        if (vt != null) {
//                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
//                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                                downhuoche++;
//                            else if (vt == 8) {
//                                downweihuaping++;
//                                downhuoche++;
//                            }
//                        } else {
//                            if (vet >= 1 && vet <= 4) downkeche++;
//                            else if (vet >= 11 && vet <= 16)
//                                downhuoche++;
//                            else {
//                                n--;
//                                xiaxing2--;
//                            }
//                        }
//                        if (v.getSpecialFlag() != null) {
//                            String[] sd = v.getSpecialFlag().split(";");
//                            for (String s : sd)
//                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                    downzhongxinghuoche++;
//                        }
//                    }
//                }
//            }
//            if (n > 0 && (endM - startM) > 0) {
//                double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
//                double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
//                List<sectionLosDirPieces> sps = new ArrayList<>();
//
//                sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));
//
//                sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
//                slsList.add(new sectionLos(" ", startStake + "-" + endStake, sps));
//            }
//            return new sectionLosResult(200, "（HBASE)查找固定桩号成功", slsList, true);
//
//        } else {
//
//
//            startMi = "K1016+20";
//            endMi = "K1030+448";
//            index = startMi.indexOf("+");
//            startMil = (index != -1) ? startMi.substring(0, index) : startMi;
//            index1 = endMi.indexOf("+");
//            endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;
//
//            n = 0;
//            sum = 0;
//            zn = 0;
//            zsum = 0;
//
//
//            List<sectionLos> slsList = new ArrayList<>();
//
//            //从起始时间到终止时间
//            startM = Integer.parseInt(startMil.substring(1));//前四个数字
//            endM = Integer.parseInt(endMil.substring(1)) + 1;
//            System.out.println("startMil.substring(1):" + startMil.substring(1) + "  endMil.substring(1):" + endMil.substring(1) + "  startM:" + startM + "  endM:" + endM);
//            if (startM > endM) {
//                int temp = endM;
//                endM = startM;
//                startM = temp;
//            }
//            for (long i = st; i < tt; i += 60000) {
//                for (int j = startM; j < endM; j++) {
//                    List<VehicleSeg> l = totalOps.getVeByRowkeys(hbaseTool.convertToHBaseTableName(i), i + "_K" + j + "_1", i + "_K" + j + "_2");
//                    for (VehicleSeg vs : l) {
//                        //判断是否有重复出
//                        if (m.get(vs.getCarId()) == null) {
//                            m.put(vs.getCarId(), vs);
//                        } else {
//                            vs.setPlateNo(vs.getPlateNo());
//                            vs.setDirection(vs.getDirection());
//                            m.put(vs.getCarId(), vs);
//                        }
//                    }
//                }
//            }
//
//
//            for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
//
//                VehicleSeg v = entry.getValue();
//                if (v != null) {
//                    n++;
//                    sum += v.getAverageSpeed();
//
//
//                    if (v.getDirection() == 1) {
//                        shangxing1++;
//                        System.out.println(shangxing1);
//                        shangxingSum += v.getAverageSpeed();
//                        Integer vt = v.getOriginalType();
//                        Integer vet = v.getVehicleType();
//                        if (vt != null) {
//                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
//                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                                uphuoche++;
//                            else if (vt == 8) {
//                                upweihuaping++;
//                                uphuoche++;
//                            }
//                        } else {
//                            if (vet >= 1 && vet <= 4) upkeche++;
//                            else if (vet >= 11 && vet <= 16) uphuoche++;
//                            else {
//                                n--;
//                                shangxing1--;
//                                shangxingSum -= v.getAverageSpeed();
//                            }
//                        }
//                        if (v.getSpecialFlag() != null) {
//                            String[] sd = v.getSpecialFlag().split(";");
//                            for (String s : sd)
//                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                    upzhongxinghuoche++;
//                        }
//                    } else if (v.getDirection() == 2) {
//                        xiaxing2++;
//                        xiaxingSum += v.getAverageSpeed();
//                        Integer vt = v.getOriginalType();
//                        Integer vet = v.getVehicleType();
//                        if (vt != null) {
//                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
//                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                                downhuoche++;
//                            else if (vt == 8) {
//                                downweihuaping++;
//                                downhuoche++;
//                            }
//                        } else {
//                            if (vet >= 1 && vet <= 4) downkeche++;
//                            else if (vet >= 11 && vet <= 16)
//                                downhuoche++;
//                            else {
//                                n--;
//                                xiaxing2--;
//                            }
//                        }
//                        if (v.getSpecialFlag() != null) {
//                            String[] sd = v.getSpecialFlag().split(";");
//                            for (String s : sd)
//                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                    downzhongxinghuoche++;
//                        }
//                    }
//                }
//            }
//            if (n > 0 && (endM - startM) > 0) {
//                double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
//                double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
//                List<sectionLosDirPieces> sps = new ArrayList<>();
//
//                sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));
//
//                sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
//                slsList.add(new sectionLos("鄂北-大新", "K1016+020-K1030+448", sps));
//            }
//        }
//        startMi = "K1016+20";
//        endMi = "K1030+448";
//        index = startMi.indexOf("+");
//        startMil = (index != -1) ? startMi.substring(0, index) : startMi;
//        index1 = endMi.indexOf("+");
//        endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;
//
//        n = 0;
//        sum = 0;
//        zn = 0;
//        zsum = 0;
//
//
//        List<sectionLos> slsList = new ArrayList<>();
//
//        //从起始时间到终止时间
//        startM = Integer.parseInt(startMil.substring(1));//前四个数字
//        endM = Integer.parseInt(endMil.substring(1)) + 1;
//        System.out.println("startMil.substring(1):" + startMil.substring(1) + "  endMil.substring(1):" + endMil.substring(1) + "  startM:" + startM + "  endM:" + endM);
//        if (startM > endM) {
//            int temp = endM;
//            endM = startM;
//            startM = temp;
//        }
//
//        for (long i = st; i < tt; i += 60000) {
//            for (int j = startM; j < endM; j++) {
//                List<VehicleSeg> l = totalOps.getVeByRowkeys(hbaseTool.convertToHBaseTableName(i), i + "_K" + j + "_1", i + "_K" + j + "_2");
//                for (VehicleSeg vs : l) {
//                    //判断是否有重复出
//                    if (m.get(vs.getCarId()) == null) {
//                        m.put(vs.getCarId(), vs);
//                    } else {
//                        vs.setPlateNo(vs.getPlateNo());
//                        vs.setDirection(vs.getDirection());
//                        m.put(vs.getCarId(), vs);
//                    }
//                }
//            }
//        }
//
//
//        for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
//
//            VehicleSeg v = entry.getValue();
//            if (v != null) {
//                n++;
//                sum += v.getAverageSpeed();
//
//
//                if (v.getDirection() == 1) {
//                    shangxing1++;
//                    System.out.println(shangxing1);
//                    shangxingSum += v.getAverageSpeed();
//                    Integer vt = v.getOriginalType();
//                    Integer vet = v.getVehicleType();
//                    if (vt != null) {
//                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
//                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                            uphuoche++;
//                        else if (vt == 8) {
//                            upweihuaping++;
//                            uphuoche++;
//                        }
//                    } else {
//                        if (vet >= 1 && vet <= 4) upkeche++;
//                        else if (vet >= 11 && vet <= 16) uphuoche++;
//                        else {
//                            n--;
//                            shangxing1--;
//                            shangxingSum -= v.getAverageSpeed();
//                        }
//                    }
//                    if (v.getSpecialFlag() != null) {
//                        String[] sd = v.getSpecialFlag().split(";");
//                        for (String s : sd)
//                            if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                upzhongxinghuoche++;
//                    }
//                } else if (v.getDirection() == 2) {
//                    xiaxing2++;
//                    xiaxingSum += v.getAverageSpeed();
//                    Integer vt = v.getOriginalType();
//                    Integer vet = v.getVehicleType();
//                    if (vt != null) {
//                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
//                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
//                            downhuoche++;
//                        else if (vt == 8) {
//                            downweihuaping++;
//                            downhuoche++;
//                        }
//                    } else {
//                        if (vet >= 1 && vet <= 4) downkeche++;
//                        else if (vet >= 11 && vet <= 16)
//                            downhuoche++;
//                        else {
//                            n--;
//                            xiaxing2--;
//                        }
//                    }
//                    if (v.getSpecialFlag() != null) {
//                        String[] sd = v.getSpecialFlag().split(";");
//                        for (String s : sd)
//                            if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
//                                downzhongxinghuoche++;
//                    }
//                }
//            }
//        }
//        if (n > 0) {
//            double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
//            double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
//            List<sectionLosDirPieces> sps = new ArrayList<>();
//
//            sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));
//
//            sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
//            slsList.add(new sectionLos("大新-大悟", "K1030+448-K1043+400", sps));
//
//        }
//
//        return new sectionLosResult(200, "查找指定桩号成功", slsList, true);
//
//
//    }

 // 初始化变量
    int shangxing1 = 0;
    int xiaxing2 = 0;
    double shangxingSum = 0;
    double xiaxingSum = 0;
    int upkeche = 0, uphuoche = 0, upweihuaping = 0, upzhongxinghuoche = 0;
    int downkeche = 0, downhuoche = 0, downweihuaping = 0, downzhongxinghuoche = 0;
public sectionLosResult sectionLOSHbase(String beginTime, String endTime, String startStake, String endStake) throws IOException {
    long startTime = System.currentTimeMillis();



    // 处理桩号格式
    if (startStake != null) startStake = startStake.replace(" ", "+");
    if (endStake != null) endStake = endStake.replace(" ", "+");

    // 转换时间范围
    long st = convertToTimestamp(beginTime);
    long tt = convertToTimestamp(endTime);

    // 获取季度表扫描计划
    Map<String, HBaseTableScanner.KeyRange> scanPlan = HBaseTableScanner.generateScanPlan(st, tt);

    // 处理桩号范围
    String startMil = extractMileagePrefix(startStake);
    String endMil = extractMileagePrefix(endStake);
    int startM = extractMileageNumber(startMil);
    int endM = extractMileageNumber(endMil) + 1;

    if (startM > endM) {
        int temp = endM;
        endM = startM;
        startM = temp;
    }

    // 存储所有车辆数据
    Map<Long, VehicleSeg> vehicleMap = new HashMap<>();

    // 扫描所有季度表
    for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {
        String tableName = entry.getKey();
        String startKey = entry.getValue().getStartKey();
        String endKey = entry.getValue().getEndKey();

        // 扫描该表的时间范围
        List<VehicleSeg> segs = scanTableByStakeRange(tableName, startKey, endKey, startM, endM);
        mergeVehicleSegs(vehicleMap, segs);
    }

    // 统计车辆数据
    for (VehicleSeg v : vehicleMap.values()) {
        if (v == null) continue;

        if (v.getDirection() == 1) {
            shangxing1++;
            shangxingSum += v.getAverageSpeed();
            countVehicleType(v, true);
        } else if (v.getDirection() == 2) {
            xiaxing2++;
            xiaxingSum += v.getAverageSpeed();
            countVehicleType(v, false);
        }
    }

    // 计算密度和LOS
    List<sectionLos> resultList = new ArrayList<>();
    if (shangxing1 > 0 || xiaxing2 > 0) {
        double upDensity = shangxing1 / ((endM - startM) * 4.0);
        double downDensity = xiaxing2 / ((endM - startM) * 4.0);

        List<sectionLosDirPieces> dirPieces = new ArrayList<>();
        dirPieces.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));
        dirPieces.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));

        String sectionName = "路段";
        if (!startStake.isEmpty() && !endStake.isEmpty()) {
            sectionName = startStake + "-" + endStake;
        }

        resultList.add(new sectionLos(sectionName, sectionName, dirPieces));
    }

    return new sectionLosResult(200, "查询成功", resultList, true);
}

/**
 * 扫描指定表和时间范围的车辆数据
 */
private List<VehicleSeg> scanTableByStakeRange(String tableName, String startKey, String endKey,
                                              int startMileage, int endMileage) {
    List<VehicleSeg> allSegs = new ArrayList<>();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
    conf.set("hbase.zookeeper.property.clientPort", "2181");

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        if (!isTableExists(connection, tableName)) {
            return allSegs;
        }

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(endKey));

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String rowKey = Bytes.toString(result.getRow());

                    // 解析桩号: rowkey格式为 "时间戳_K桩号_方向"
                    String[] parts = rowKey.split("_");
                    if (parts.length < 3) continue;

                    String stakePart = parts[1];
                    if (!stakePart.startsWith("K")) continue;

                    int stakeNum = Integer.parseInt(stakePart.substring(1));
                    if (stakeNum < startMileage || stakeNum >= endMileage) continue;

                    // 解析车辆数据
                    List<VehicleSeg> segs = parseVehicleSegs(result);
                    allSegs.addAll(segs);
                }
            }
        }
    } catch (IOException e) {
        System.err.println("HBase扫描失败: " + e.getMessage());
    }

    return allSegs;
}

/**
 * 解析HBase结果中的车辆数据
 */
private List<VehicleSeg> parseVehicleSegs(Result result) {
    List<VehicleSeg> segs = new ArrayList<>();
    byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
    if (valueBytes == null) return segs;

    try {
        String json = Bytes.toString(valueBytes);
        JSONArray array = JSON.parseArray(json);

        // 从rowkey解析方向
        String rowKey = Bytes.toString(result.getRow());
        String[] parts = rowKey.split("_");
        int direction = Integer.parseInt(parts[2]);

        for (Object obj : array) {
            VehicleSeg seg = JSON.parseObject(obj.toString(), VehicleSeg.class);
            seg.setDirection(direction);
            segs.add(seg);
        }
    } catch (Exception e) {
        System.err.println("解析车辆数据失败: " + e.getMessage());
    }

    return segs;
}

/**
 * 合并车辆数据
 */
private void mergeVehicleSegs(Map<Long, VehicleSeg> map, List<VehicleSeg> segs) {
    for (VehicleSeg seg : segs) {
        VehicleSeg existing = map.get(seg.getCarId());
        if (existing == null) {
            map.put(seg.getCarId(), seg);
        } else {
            // 合并逻辑（根据业务需求）
            existing.setPointSum(existing.getPointSum() + seg.getPointSum());
            existing.setSpeedSum(existing.getSpeedSum() + seg.getSpeedSum());
            existing.setAverageSpeed(existing.getSpeedSum() / existing.getPointSum());
        }
    }
}

/**
 * 统计车辆类型
 */
private void countVehicleType(VehicleSeg v, boolean isUpDirection) {
    Integer vt = v.getOriginalType();
    Integer vet = v.getVehicleType();

    if (vt != null) {
        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) {
            if (isUpDirection) upkeche++;
            else downkeche++;
        } else if (vt == 2 || (vt >= 10 && vt <= 11) || (vt >= 170 && vt <= 177)) {
            if (isUpDirection) uphuoche++;
            else downhuoche++;
        } else if (vt == 8) {
            if (isUpDirection) {
                upweihuaping++;
                uphuoche++;
            } else {
                downweihuaping++;
                downhuoche++;
            }
        }
    } else if (vet != null) {
        if (vet >= 1 && vet <= 4) {
            if (isUpDirection) upkeche++;
            else downkeche++;
        } else if (vet >= 11 && vet <= 16) {
            if (isUpDirection) uphuoche++;
            else downhuoche++;
        }
    }

    // 处理特殊标志
    if (v.getSpecialFlag() != null) {
        String[] flags = v.getSpecialFlag().split(";");
        for (String flag : flags) {
            if (flag.equals("20") || flag.equals("21") || flag.equals("22") || flag.equals("23")) {
                if (isUpDirection) upzhongxinghuoche++;
                else downzhongxinghuoche++;
            }
        }
    }
}

// 辅助方法
private String extractMileagePrefix(String stake) {
    if (stake == null || stake.isEmpty()) return "";
    int index = stake.indexOf("+");
    return (index != -1) ? stake.substring(0, index) : stake;
}

private int extractMileageNumber(String stake) {
    if (stake == null || stake.isEmpty()) return 0;
    if (stake.startsWith("K")) {
        return Integer.parseInt(stake.substring(1));
    }
    return Integer.parseInt(stake);
}

private boolean isTableExists(Connection connection, String tableName) throws IOException {
    try (Admin admin = connection.getAdmin()) {
        return admin.tableExists(TableName.valueOf(tableName));
    }
}

    public sectionLosResult sectionLOSRedis(String beginTime, String endTime, String startStake, String endStake) {

        long t1 = System.currentTimeMillis();
        //region Description
        if (startStake != null) startStake = startStake.replace(" ", "+");
        if (endStake != null) endStake = endStake.replace(" ", "+");

        int shangxing1 = 0;
        int xiaxing2 = 0;//下行车辆数
        String startMi = startStake;
        String endMi = endStake;//桩号（完整版）
        int startM = 0;
        int endM = 0;//桩号中的数字
        long st = convertToTimestamp(beginTime) / 1000 * 1000;
        long tt = convertToTimestamp(endTime) / 1000 * 1000 + 1000;
        int upkeche = 0;
        int uphuoche = 0;
        int upweihuaping = 0;
        int upzhongxinghuoche = 0;
        int downkeche = 0;
        int downhuoche = 0;
        int downweihuaping = 0;
        int downzhongxinghuoche = 0;
        int zupkeche = 0;
        int zuphuoche = 0;
        int zupweihuaping = 0;
        int zupzhongxinghuoche = 0;
        double shangxingSum = 0;
        double xiaxingSum = 0;//下行平均速度总和
        int index;
        int index1;
        String startMil;
        String endMil;
        double n = 0;
        double sum = 0;
        double zn = 0;
        double zsum = 0;

        Map<Long, VehicleSeg> m = new HashMap<>();
        //第一次计算，只算单个的
        if (startMi != null && endMi != null) {
            List<sectionLos> slsList = new ArrayList<>();

            index = startMi.indexOf("+");
            startMil = (index != -1) ? startMi.substring(0, index) : startMi;
            ;
            index1 = endMi.indexOf("+");
            endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;
            //从起始时间到终止时间
            startM = Integer.parseInt(startMil.substring(1));//前四个数字
            endM = Integer.parseInt(endMil.substring(1)) + 1;
            //startMil.substring(1):1054  endMil.substring(1):1048  startM:1054  endM:1049
            if (startM > endM) {
                int temp = endM;
                endM = startM;
                startM = temp;
            }
            //                System.out.println("startMil:" + startMil + "   endMil:" + endMil + "  startM:" + startM + "   endm:" + endM + "st:" + st + "tt" + tt);//startMil:K1054   endMil:K1048  startM:1049   endm:1054
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v" + i + "_K" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
                    for (VehicleSeg vs : l) {
                        //判断是否有重复出
                        if (m.get(vs.getCarId()) == null) {
                            m.put(vs.getCarId(), vs);
                        } else {
                            vs.setDirection(vs.getDirection());
                            m.put(vs.getCarId(), vs);
                        }
                    }
                }
            }
            for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    n++;
                    sum += v.getAverageSpeed();


                    if (v.getDirection() == 1) {
                        shangxing1++;
                        System.out.println(shangxing1);
                        shangxingSum += v.getAverageSpeed();

                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                uphuoche++;
                            else if (vt == 8) {
                                upweihuaping++;
                                uphuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) upkeche++;
                            else if (vet >= 11 && vet <= 16) uphuoche++;
                            else {
                                n--;
                                shangxing1--;
                                shangxingSum -= v.getAverageSpeed();
                            }
                        }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    upzhongxinghuoche++;
                        }
                    } else if (v.getDirection() == 2) {
                        xiaxing2++;
                        xiaxingSum += v.getAverageSpeed();
                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                downhuoche++;
                            else if (vt == 8) {
                                downweihuaping++;
                                downhuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) downkeche++;
                            else if (vet >= 11 && vet <= 16)
                                downhuoche++;
                            else {
                                n--;
                                xiaxing2--;
                            }
                        }
                    }
                }
            }
            // 在getRedis方法中添加LOS计算逻辑
            // 在计算完密度后添加以下代码
            // 主路LOS计算 (假设主路为双向8车道，每个方向4车道)
            if (n > 0 && (endM - startM) > 0) {
                double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
                double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
                List<sectionLosDirPieces> sps = new ArrayList<>();

                sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));

                sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
                slsList.add(new sectionLos(" ", startStake + "-" + endStake, sps));
            }
            return new sectionLosResult(200, "查找指定桩号成功", slsList, true);


        } else {

            List<sectionLos> slsList = new ArrayList<>();

            startMi = "K1016+20";
            endMi = "K1030+448";
            index = startMi.indexOf("+");
            startMil = startMi.substring(0, index);
            ;
            index1 = endMi.indexOf("+");
            endMil = endMi.substring(0, index1);
            //从起始时间到终止时间
            startM = Integer.parseInt(startMil.substring(1));//前四个数字
            endM = Integer.parseInt(endMil.substring(1)) + 1;
            //startMil.substring(1):1054  endMil.substring(1):1048  startM:1054  endM:1049
            //                System.out.println("startMil:" + startMil + "   endMil:" + endMil + "  startM:" + startM + "   endm:" + endM + "st:" + st + "tt" + tt);//startMil:K1054   endMil:K1048  startM:1049   endm:1054
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v" + i + "_K" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
                    for (VehicleSeg vs : l) {
                        //判断是否有重复出
                        if (m.get(vs.getCarId()) == null) {
                            m.put(vs.getCarId(), vs);
                        } else {
                            vs.setDirection(vs.getDirection());
                            m.put(vs.getCarId(), vs);
                        }
                    }
                }
            }
            for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    n++;
                    sum += v.getAverageSpeed();


                    if (v.getDirection() == 1) {
                        shangxing1++;
                        shangxingSum += v.getAverageSpeed();

                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                uphuoche++;
                            else if (vt == 8) {
                                upweihuaping++;
                                uphuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) upkeche++;
                            else if (vet >= 11 && vet <= 16) uphuoche++;
                            else {
                                n--;
                                shangxing1--;
                                shangxingSum -= v.getAverageSpeed();
                            }
                        }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    upzhongxinghuoche++;
                        }
                    } else if (v.getDirection() == 2) {
                        xiaxing2++;
                        xiaxingSum += v.getAverageSpeed();
                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                downhuoche++;
                            else if (vt == 8) {
                                downweihuaping++;
                                downhuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) downkeche++;
                            else if (vet >= 11 && vet <= 16)
                                downhuoche++;
                            else {
                                n--;
                                xiaxing2--;
                            }
                        }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    downzhongxinghuoche++;
                        }
                    }
                }
            }

            // 在getRedis方法中添加LOS计算逻辑
            // 在计算完密度后添加以下代码
            // 主路LOS计算 (假设主路为双向8车道，每个方向4车道)
            if (n > 0) {
                double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
                double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
                List<sectionLosDirPieces> sps = new ArrayList<>();

                sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));
                System.out.println("n:" + n + "   " + shangxing1 + "/((" + endM + "-" + startM + ")*" + "4");
                System.out.println("n: " + n + "   " + xiaxing2 + "/((" + endM + "-" + startM + ")*" + "4");
                sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
                slsList.add(new sectionLos("鄂北-大新", "K1016+020-K1030+448", sps));
            }


            //第二次计算，第二段的


            startMi = "K1030+448";
            endMi = "K1043+400";
            index = startMi.indexOf("+");
            startMil = startMi.substring(0, index);
            ;
            index1 = endMi.indexOf("+");
            endMil = endMi.substring(0, index1);
            //从起始时间到终止时间
            startM = Integer.parseInt(startMil.substring(1));//前四个数字
            endM = Integer.parseInt(endMil.substring(1)) + 1;
            //startMil.substring(1):1054  endMil.substring(1):1048  startM:1054  endM:1049
            if (startM > endM) {
                int temp = endM;
                endM = startM;
                startM = temp;
            }
            //                System.out.println("startMil:" + startMil + "   endMil:" + endMil + "  startM:" + startM + "   endm:" + endM + "st:" + st + "tt" + tt);//startMil:K1054   endMil:K1048  startM:1049   endm:1054
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v" + i + "_K" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
                    for (VehicleSeg vs : l) {
                        //判断是否有重复出
                        if (m.get(vs.getCarId()) == null) {
                            m.put(vs.getCarId(), vs);
                        } else {
                            vs.setDirection(vs.getDirection());
                            m.put(vs.getCarId(), vs);
                        }
                    }
                }
            }
            for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    n++;
                    sum += v.getAverageSpeed();


                    if (v.getDirection() == 1) {
                        shangxing1++;
                        System.out.println(shangxing1);
                        shangxingSum += v.getAverageSpeed();

                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                uphuoche++;
                            else if (vt == 8) {
                                upweihuaping++;
                                uphuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) upkeche++;
                            else if (vet >= 11 && vet <= 16) uphuoche++;
                            else {
                                n--;
                                shangxing1--;
                                shangxingSum -= v.getAverageSpeed();
                            }
                        }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    upzhongxinghuoche++;
                        }
                    } else if (v.getDirection() == 2) {
                        xiaxing2++;

                        xiaxingSum += v.getAverageSpeed();
                        Integer vt = v.getOriginalType();
                        Integer vet = v.getVehicleType();
                        if (vt != null) {
                            if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
                            else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                                downhuoche++;
                            else if (vt == 8) {
                                downweihuaping++;
                                downhuoche++;
                            }
                        } else {
                            if (vet >= 1 && vet <= 4) downkeche++;
                            else if (vet >= 11 && vet <= 16)
                                downhuoche++;
                            else {
                                n--;
                                xiaxing2--;
                            }
                        }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    downzhongxinghuoche++;
                        }
                    }
                }
            }
            // 在getRedis方法中添加LOS计算逻辑
            // 在计算完密度后添加以下代码
            // 主路LOS计算 (假设主路为双向8车道，每个方向4车道)
            if (n > 0) {
                double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
                double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度
                List<sectionLosDirPieces> sps = new ArrayList<>();

                sps.add(new sectionLosDirPieces("1", calculateLOS(upDensity), upDensity));

                sps.add(new sectionLosDirPieces("2", calculateLOS(downDensity), downDensity));
                slsList.add(new sectionLos("大新-大悟", "K1030+448-K1043+400", sps));
            }
            return new sectionLosResult(200, "(REDIS)查找固定桩号成功", slsList, true);

        }
    }

    private String calculateLOS(double density) {
        if (density <= 7) return "A";
        else if (density <= 11) return "B";
        else if (density <= 16) return "C";
        else if (density <= 22) return "D";
        else if (density <= 28) return "E";
        else return "F";
    }

    @Override
    public sectionLosResult zaSectionLOS(String beginTime, String endTime, String facilitiesId) throws IOException {
        long t1 = System.currentTimeMillis();
        //region Description


        int shangxing1 = 0;
        int xiaxing2 = 0;//下行车辆数

        int startM = 0;
        int endM = 0;//桩号中的数字
        long st = convertToTimestamp(beginTime) / 1000 * 1000;
        long tt = convertToTimestamp(endTime) / 1000 * 1000 + 1000;
        int upkeche = 0;
        int uphuoche = 0;
        int upweihuaping = 0;
        int upzhongxinghuoche = 0;
        int downkeche = 0;
        int downhuoche = 0;
        int downweihuaping = 0;
        int downzhongxinghuoche = 0;
        int zupkeche = 0;
        int zuphuoche = 0;
        int zupweihuaping = 0;
        int zupzhongxinghuoche = 0;
        double shangxingSum = 0;
        double xiaxingSum = 0;//下行平均速度总和
        int index;
        int index1;
        String startMil;
        String endMil;
        double n = 0;
        double sum = 0;
        double zn = 0;
        double zsum = 0;
        String zaStartMil = "";
        String zaEndMil = "";
        TimeSpatialData kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0", "0");
        TimeSpatialResult t2 = new TimeSpatialResult(200, "内存查找————无范围内数据,原因：桩号转换失败", kong, true);
        TimeSpatialData tos = new TimeSpatialData();
        boolean za = true;
        boolean main = true;
        stakeEnvents.StakeAssignment stakeAssign;
        Map<Long, VehicleSeg> mergedMap = new HashMap<>();
        Map<Long, VehicleSeg> m = new HashMap<>();
        Map<Long, VehicleSeg> am = new HashMap<>();
        Map<Long, VehicleSeg> bm = new HashMap<>();
        Map<Long, VehicleSeg> cm = new HashMap<>();
        Map<Long, VehicleSeg> dm = new HashMap<>();
//Set<String> keys = redisTemplate.keys("v*");
//            System.out.println("keys: "+keys);
//        System.out.println(System.currentTimeMillis());
        {
//            System.out.println("开始数据库查找，查找语句：http://100.65.38.139:8080/getByTimeSpatialWithID?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
            shangxing1 = 0;
            xiaxing2 = 0;//下行车辆数
            shangxingSum = 0;
            xiaxingSum = 0;//下行平均速度总和1746857652000

            startM = 0;
            endM = 0;//桩号中的数字
            upkeche = 0;
            uphuoche = 0;
            upweihuaping = 0;
            upzhongxinghuoche = 0;
            downkeche = 0;
            downhuoche = 0;
            downweihuaping = 0;
            downzhongxinghuoche = 0;
            zupkeche = 0;
            zuphuoche = 0;
            zupweihuaping = 0;
            zupzhongxinghuoche = 0;


            st = st / 60000 * 60000;
            tt = tt / 60000 * 60000 + 60000;
            zaStartMil = "";
            zaEndMil = "";
            kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0", "0");
            t2 = new TimeSpatialResult(200, "8001————无范围内数据,原因：桩号转换失败", kong, true);
            tos = new TimeSpatialData();
            za = true;
            main = true;

            n = 0;
            sum = 0;
            zn = 0;
            zsum = 0;


            List<sectionLosDirPieces> sls=new ArrayList<>();
            //AK
                String startSK = LocationOP.GETLonNearest(114.03852081298828, roadAKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(114.04580688476562, roadAKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("AK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
//                System.out.println("AK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;
                for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_AK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (am.get(vs.getCarId()) == null) {
                                am.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = am.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                am.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
                 for (Map.Entry<Long, VehicleSeg> entry : am.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    zn++;
                    zsum += v.getAverageSpeed();

                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    if (vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            zuphuoche++;
                        else if (vt == 8) {
                            zupweihuaping++;
                            zuphuoche++;
                        }
                    } else {
                        if (vet >= 1 && vet <= 4) zupkeche++;
                        else if (vet >= 11 && vet <= 16)
                            zuphuoche++;
                        else {
                            zn--;
                            zsum -= v.getAverageSpeed();
                        }
                    }
                }
            }
                 sls.add(new sectionLosDirPieces("A",calculateLOS(zn/ ((endM - startM) * 4.0)), zn/ ((endM - startM) * 4.0)));

            //BK
                startSK = LocationOP.GETLonNearest(114.03852081298828, roadBKDataList).getLocation();
               endSK = LocationOP.GETLonNearest(114.04602813720703, roadBKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("BK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
//                System.out.println("BK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;

                for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_BK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (bm.get(vs.getCarId()) == null) {
                                bm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = bm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                bm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
                 for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_BK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (bm.get(vs.getCarId()) == null) {
                                bm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = bm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                bm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
                 for (Map.Entry<Long, VehicleSeg> entry : bm.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    zn++;
                    zsum += v.getAverageSpeed();

                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    if (vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            zuphuoche++;
                        else if (vt == 8) {
                            zupweihuaping++;
                            zuphuoche++;
                        }
                    } else {
                        if (vet >= 1 && vet <= 4) zupkeche++;
                        else if (vet >= 11 && vet <= 16)
                            zuphuoche++;
                        else {
                            zn--;
                            zsum -= v.getAverageSpeed();
                        }
                    }
                }
            }
                 sls.add(new sectionLosDirPieces("B",calculateLOS(zn/ ((endM - startM) * 4.0)), zn/ ((endM - startM) * 4.0)));


            //CK
                startSK = LocationOP.GETLonNearest(114.0416030883789, roadCKDataList).getLocation();
                endSK = LocationOP.GETLonNearest(114.04431915283203, roadCKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("CK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
//                System.out.println("CK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;
                for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_CK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (cm.get(vs.getCarId()) == null) {
                                cm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = cm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                cm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
             for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_CK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (cm.get(vs.getCarId()) == null) {
                                cm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = cm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                cm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
             zn=0;
                 for (Map.Entry<Long, VehicleSeg> entry : cm.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    zn++;
                    zsum += v.getAverageSpeed();

                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    if (vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            zuphuoche++;
                        else if (vt == 8) {
                            zupweihuaping++;
                            zuphuoche++;
                        }
                    } else {
                        if (vet >= 1 && vet <= 4) zupkeche++;
                        else if (vet >= 11 && vet <= 16)
                            zuphuoche++;
                        else {
                            zn--;
                            zsum -= v.getAverageSpeed();
                        }
                    }
                }
            }
                 sls.add(new sectionLosDirPieces("C",calculateLOS(zn/ ((endM - startM) * 4.0)), zn/ ((endM - startM) * 4.0)));

            //DK
                startSK = LocationOP.GETLonNearest(114.0438003540039, roadDKDataList).getLocation();
                endSK = LocationOP.GETLonNearest(114.04520416259766, roadDKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("DK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
//                System.out.println("DK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;
                zn=0;
                for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_DK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (dm.get(vs.getCarId()) == null) {
                                dm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = dm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                dm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
             for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_DK" + j);
                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (dm.get(vs.getCarId()) == null) {
                                 dm.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan =  dm.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                 dm.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
                 for (Map.Entry<Long, VehicleSeg> entry :  dm.entrySet()) {
                VehicleSeg v = entry.getValue();
                if (v != null) {
                    zn++;
                    zsum += v.getAverageSpeed();

                    Integer vt = v.getOriginalType();
                    Integer vet = v.getVehicleType();
                    if (vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            zuphuoche++;
                        else if (vt == 8) {
                            zupweihuaping++;
                            zuphuoche++;
                        }
                    } else {
                        if (vet >= 1 && vet <= 4) zupkeche++;
                        else if (vet >= 11 && vet <= 16)
                            zuphuoche++;
                        else {
                            zn--;
                            zsum -= v.getAverageSpeed();
                        }
                    }
                }
            }
                 sls.add(new sectionLosDirPieces("D",calculateLOS(zn/ ((endM - startM) * 4.0)), zn/ ((endM - startM) * 4.0)));

List<sectionLos>slsList=new ArrayList<>();
slsList.add(new sectionLos("大悟南枢纽","K1062+700",sls));

                 return new sectionLosResult(200, "(REDIS)查找固定桩号成功", slsList, true);


        }
    }


    @Override
    public zaEachSitResult zaEachSit(List<String> stationIds, String beginTime, String endTime, int level) throws Exception {

            // 解析时间参数
            LocalDateTime startTime = LocalDateTime.parse(beginTime, INPUT_FORMATTER);
            LocalDateTime endingTime = LocalDateTime.parse(endTime, INPUT_FORMATTER);

            // 查询数据
            List<zaEachSitData> result = queryRampTrafficData(startTime, endingTime);

            return new zaEachSitResult(200,"success",result,true);


    }
   @Override
    public flowNo getFlowNo(String startTime, String endTime, String startStake, String endStake) throws IOException {
        List<totalOpsv3.PeriodTrafficCounts> cl=new ArrayList<>();

        List<String> sections1 = getPassedSections(startStake, endStake);
        for(String section : sections1) {
            String stStake="";
            String edStake="";
            for(HighwaySectionUtils.HighwaySection section1:SECTIONS){
                if(section1.getStartName().equals(section.substring(0,2))){
                    stStake=section1.getStartStake();
                    edStake=section1.getEndStake();
                    totalOpsv3.PeriodTrafficCounts counts = totalOpsv3.getTrafficCountWithPreviousPeriod(startTime, endTime, stStake, edStake, section1.getStartName()+"-"+section1.getEndName());
                    counts.setStake(stStake+"-"+edStake);
                    cl.add(counts);
                }
            }
        }
        List<Integer>list=new ArrayList<>();


        for(int i = 0 ; i < cl.size(); i++){
            list.add(cl.get(i).getCurrentDirection1()+cl.get(i).getCurrentDirection2());
        }
        List<Integer>no=calculateRanks(list);
        List<flowNo.SectionInfo>sl=new ArrayList<>();
          for(int i = 0 ; i < cl.size(); i++){
            totalOpsv3.PeriodTrafficCounts c=cl.get(no.get(i));
            List<flowNo.DirectionInfo>d=new ArrayList<>();
            d.add(new flowNo.DirectionInfo(1,c.getCurrentDirection1(),0+"%",(c.getCurrentDirection1()-c.getPreviousDirection1())/c.getPreviousDirection1()+"%"));
            d.add(new flowNo.DirectionInfo(2,c.getCurrentDirection2(),0+"%",(c.getCurrentDirection2()-c.getPreviousDirection2())/c.getPreviousDirection2()+"%"));
            sl.add(new flowNo.SectionInfo(c.getName(),c.getStake(),d));
          }

        return new flowNo(200, "成功", new flowNo.SectionTrafficData(sl), true);
    }
    public static List<Integer> calculateRanks(List<Integer> list) {
        // 创建一个副本用于排序而不影响原始列表
        List<Integer> sortedList = new ArrayList<>(list);
        // 降序排序
        Collections.sort(sortedList, Collections.reverseOrder());

        // 创建一个映射来存储每个元素的排名
        Map<Integer, Integer> rankMap = new HashMap<>();
        int rank = 1;
        for (int i = 0; i < sortedList.size(); i++) {
            // 如果当前元素与前一个元素不同，更新排名
            if (i == 0 || !sortedList.get(i).equals(sortedList.get(i - 1))) {
                rank = i + 1;
            }
            rankMap.put(sortedList.get(i), rank);
        }

        // 创建结果列表，存储原始列表中每个元素的排名
        List<Integer> result = new ArrayList<>();
        for (int num : list) {
            result.add(rankMap.get(num)-1);
        }

        return result;
    }
    @Override
    public carNumResult getCarNumber(String beginTime, String endTime, String startStake, String endStake, int level) throws IOException {
        if(startStake.isEmpty()||endStake.isEmpty()){
            List<totalOpsv3.TrafficResult> tl=new ArrayList<>();
            List<HighwaySectionUtils.SectionStakeInfo> allSections = getAllSectionStakes();
            for(HighwaySectionUtils.SectionStakeInfo section:allSections){
                    totalOpsv3.TrafficResult result = totalOpsv3.queryTrafficStats(
                    beginTime,
                    endTime,
                    section.getStartStake(),
                    section.getEndStake(),
                    level,
                   section.sectionName
                );
                tl.add(result);
            }
            tl.add(totalOpsv3.queryTrafficStats(beginTime,endTime,"K1016+020","K1173+790",level,"全线"));

        return new carNumResult(200, "成功，返回路段："+allSections, true, tl);

        }else{
            List<totalOpsv3.TrafficResult> tl=new ArrayList<>();
        List<String> sections1 = getPassedSections(startStake, endStake);//路段名称列表
        for(String section : sections1) {
            String stStake="";
            String edStake="";
            String name="";
            for(HighwaySectionUtils.HighwaySection section1:SECTIONS){
                if(section1.getStartName().equals(section.substring(0,2))){
                    stStake=section1.getStartStake();
                    edStake=section1.getEndStake();
                    name=section1.getStartName()+"-"+section1.getEndName();
                     totalOpsv3.TrafficResult result = totalOpsv3.queryTrafficStats(
                    beginTime,
                    endTime,
                    stStake,
                    edStake,
                    level,
                    name
                );
                tl.add(result);
                }
            }

        }
        return new carNumResult(200, "成功，返回路段："+sections1, true, tl);
        }



    }

}




