package com.ljj.flinkquery.demos.web.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.entity.data.seventhData;
import com.ljj.flinkquery.demos.entity.data.seventhResult;
import com.ljj.flinkquery.demos.entity.watch.*;
import com.ljj.flinkquery.demos.web.impl.edu.hbaseTool;
import com.ljj.flinkquery.demos.web.impl.edu.querys.TollStationFlowCalculator;
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
import java.text.DecimalFormat;
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
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.*;
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
    public seventhResult getBatchUpDownCharger(List<String> stationIds, String beginTime, String endTime) {
        List<seventhData> l = new ArrayList<>();
        System.out.println("====sts:"+stationIds);
        for (String stationId : stationIds) {
            l.add(getUpDownChargerByDuration1(stationId, beginTime, endTime));
        }
        seventhResult l1 = new seventhResult(200, "查询成功",  true,l);
        return l1;
    }

    @Override
    public totalResult getHolyTotal(Long timestamp) throws IOException, ExecutionException, InterruptedException {
        long st = System.currentTimeMillis();
        System.out.println("getHolyTotal.start: " + st);
        int sum = 0;
        int upSum = 0;
        int downSum = 0;
        int busUp = 0;
        int busDown = 0;
        int trackUp = 0;
        int trackDown = 0;
         int shangxing1 = 0;
    int xiaxing2 = 0;
    double shangxingSum = 0;
    double xiaxingSum = 0;
    int upkeche = 0, uphuoche = 0, upweihuaping = 0, upzhongxinghuoche = 0;
    int downkeche = 0, downhuoche = 0, downweihuaping = 0, downzhongxinghuoche = 0;
           // 1. 获取所有v2_开头的键
            Set<String> keys = redisTemplate1.keys("v2_*");
            Set<String> timesK = new HashSet<>();
            Set<String> timesA = new HashSet<>();
            Set<String> timesB = new HashSet<>();
            Set<String> timesC = new HashSet<>();
            Set<String> timesD = new HashSet<>();
            for (String key : keys) {
                String[] l = key.split("_");
                if (l[2].charAt(0) != 'K') {
                    if (l[2].charAt(0) == 'A') timesA.add(l[1]);
                    if (l[2].charAt(0) == 'B') timesB.add(l[1]);
                    if (l[2].charAt(0) == 'C') timesC.add(l[1]);
                    if (l[2].charAt(0) == 'D') timesD.add(l[1]);
                } else timesK.add(l[1]);
            }

        Map<Long, VehicleSeg> m = new HashMap<>();

             List<Long> list = new ArrayList<>();
                for (String key : timesK) list.add(Long.parseLong(key) / 1000 * 1000);
//                System.out.println("startMil:" + startMil + "   endMil:" + endMil + "  startM:" + startM + "   endm:" + endM + "st:" + st + "tt" + tt);//startMil:K1054   endMil:K1048  startM:1049   endm:1054
//                System.out.println("startM:" + startM + " endM:" + endM + " iiii:" + list);
                for (long i : list) {

                for (int j = 1016; j < 1175; j++) {
//                    for (int j = startM; j < endM; j++) {

                        String redisKey = "v2_" + i + "_K" + j;
                        List<VehicleSeg> l = ge(redisTemplate, redisKey);

                        for (VehicleSeg vs : l) {
                            //判断是否有重复出
                            if (m.get(vs.getCarId()) == null) {
                                m.put(vs.getCarId(), vs);
                            } else {
                                VehicleSeg yuan = m.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(vs.getDirection());
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                m.put(vs.getCarId(), vs);
                            }
                        }
                    }
                }
                double n = 0;
        for (Map.Entry<Long, VehicleSeg> entry : m.entrySet()){

                VehicleSeg v = entry.getValue();
                if (v != null) {
                    n++;

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
                        }else{
                       if (vet>=1&&vet<=4) upkeche++;
                    else if (vet >= 11 && vet<= 16)
                        uphuoche++;else {n--;shangxing1--;shangxingSum -= v.getAverageSpeed();}
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
                        }else{
                       if (vet>=1&&vet<=4) downkeche++;
                    else if (vet >= 11 && vet<= 16)
                        downhuoche++;
                    else {n--;xiaxing2--;xiaxingSum -= v.getAverageSpeed();}
                }
                    }
                }
            }
        System.out.println("upSum:" + shangxing1 + "downSum:" + xiaxing2 + "busUp:" + upkeche + "busDown:" + downkeche);
        secondResult second = new secondResult(shangxing1, xiaxing2, upkeche, uphuoche, downkeche, downhuoche,
                Math.round((((double) upkeche / shangxing1) * 100.0)) / 100.0,
                Math.round((((double) uphuoche / shangxing1) * 100.0)) / 100.0,
                Math.round((((double) downkeche / xiaxing2) * 100.0)) / 100.0,
                Math.round((((double) downhuoche / xiaxing2) * 100.0)) / 100.0);

        int asd = getDayOfYear(timestamp);
        int[] ad = getYearToDateTrafficMemory();
        //数量，时间
        Pair<Integer, Long> np = getTodayTotalMemory(timestamp);
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
//        if (abs(startTime - currentTime) < 20000) {
//            return sectionLOSRedis(beginTime, endTime1, startStake, endStake);
//        }
//        if (startTime > timeSplit) {
//            return sectionLOSRedis(beginTime, endTime1, startStake, endStake);
//        } else if (endTime > timeSplit & startTime < timeSplit) {
//            return new sectionLosResult(200, "内存、数据库同时查询", sectionLOSRedis(beginTime, endTime1, startStake, endStake).getData(), true);
//        } else if (endTime < timeSplit) {
            return sectionLOSHbase(beginTime, endTime1, startStake, endStake);
//        }
//        return new sectionLosResult(200, "起始时间必须大于终止时间", null, true);

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
//        TimeSpatialData kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0","0","0");
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
//        kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0","0","0");
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
    System.out.println("scanPlan:"+scanPlan);
    List<sectionLos> resultList = new ArrayList<>();

    if(!Objects.equals(startStake, "") && !Objects.equals(endStake, "")){//如果起始结束桩号都不是空
             List<sectionStartEndStake> sections1 = getPassedSectionStartEndStake(startStake, endStake);
            for(sectionStartEndStake section : sections1){
                for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {
                    String tableName = entry.getKey();
                    String startKey = entry.getValue().getStartKey();
                    String endKey = entry.getValue().getEndKey();
                    List<VehicleSeg> segs = getVehicleSegmentsByRange(tableName, Long.parseLong(startKey), Long.parseLong(endKey),section.getStartStake(),section.getEndStake());
                    vehicleMap=mergeVehicleSegs(vehicleMap, segs);

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
                            DecimalFormat df = new DecimalFormat("#.##"); // 也可以用 "0.00" 模式，不足两位会补零
                            String formattedNumber = df.format(shangxing1 / ((endM - startM) * 2.0));
                            String formattedNumber1 = df.format(xiaxing2 / ((endM - startM) * 2.0));
                            List<sectionLosDirPieces> dirPieces = new ArrayList<>();
                    //        System.out.println("upDensity1:" + upDensity1+ "  downDensity1:" + downDensity1+"shangxing1:" + shangxing1+"xiaxing2:" + xiaxing2+"endM:"+endM+"startM:"+startM);
                            SaturationResult saturationResult = calculateSaturation(Double.parseDouble(formattedNumber));
                            SaturationResult saturationResult1 = calculateSaturation(Double.parseDouble(formattedNumber1));
                            dirPieces.add(new sectionLosDirPieces("1", saturationResult.getLevel(), saturationResult.getSaturation()));
                            dirPieces.add(new sectionLosDirPieces("2", saturationResult1.getLevel(), saturationResult1.getSaturation()));


                    //        List<sectionLosDirPieces> dirPieces = new ArrayList<>();
                    //        dirPieces.add(new sectionLosDirPieces("1", calculateLOS(upDensity),calculateSaturation(upDensity).getSaturation()));
                    //        dirPieces.add(new sectionLosDirPieces("2", calculateLOS(downDensity), calculateSaturation(upDensity).getSaturation()));



                            String sectionName = "路段";
                            if (endStake != null && startStake != null && !startStake.isEmpty() && !endStake.isEmpty()) {
                                sectionName = startStake + "-" + endStake;
                            }
                            System.out.println("sectionName:"+sectionName+" dirPieces:"+dirPieces);
                            resultList.add(new sectionLos(section.getSectionName(), "K"+section.getStartStake()+"-"+"K"+section.getEndStake(), dirPieces));
                          shangxing1 = 0;
                         xiaxing2 = 0;
                         shangxingSum = 0;
                         xiaxingSum = 0;
                         upkeche = 0;uphuoche = 0;upweihuaping = 0;upzhongxinghuoche = 0;
                         downkeche = 0;downhuoche = 0;downweihuaping = 0;downzhongxinghuoche = 0;


                }//季度表
            }//路段表
    }else{
         List<sectionStartEndStake> sections1 = new ArrayList<>();
         sections1.add(new sectionStartEndStake("鄂北-大新", 1016, 1030));
         sections1.add(new sectionStartEndStake("大新-大悟", 1030, 1043));
            for(sectionStartEndStake section : sections1){
                System.out.println("section:"+section);
                for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {
                    String tableName = entry.getKey();
                    String startKey = entry.getValue().getStartKey();
                    String endKey = entry.getValue().getEndKey();
                    List<VehicleSeg> segs = getVehicleSegmentsByRange(tableName, Long.parseLong(startKey), Long.parseLong(endKey),section.getStartStake(),section.getEndStake());
                    vehicleMap=mergeVehicleSegs(vehicleMap, segs);

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
                    System.out.println("shangxing1:"+shangxing1+"xiaxing2:"+xiaxing2);
                        // 计算密度和LOS
                            DecimalFormat df = new DecimalFormat("#.##"); // 也可以用 "0.00" 模式，不足两位会补零
                            String formattedNumber = df.format(shangxing1 / ((endM - startM) * 2.0));
                            String formattedNumber1 = df.format(xiaxing2 / ((endM - startM) * 2.0));
                            List<sectionLosDirPieces> dirPieces = new ArrayList<>();
                    //        System.out.println("upDensity1:" + upDensity1+ "  downDensity1:" + downDensity1+"shangxing1:" + shangxing1+"xiaxing2:" + xiaxing2+"endM:"+endM+"startM:"+startM);
                            SaturationResult saturationResult = calculateSaturation(Double.parseDouble(formattedNumber));
                            SaturationResult saturationResult1 = calculateSaturation(Double.parseDouble(formattedNumber1));
                            dirPieces.add(new sectionLosDirPieces("1", saturationResult.getLevel(), saturationResult.getSaturation()));
                            dirPieces.add(new sectionLosDirPieces("2", saturationResult1.getLevel(), saturationResult1.getSaturation()));


                    //        List<sectionLosDirPieces> dirPieces = new ArrayList<>();
                    //        dirPieces.add(new sectionLosDirPieces("1", calculateLOS(upDensity),calculateSaturation(upDensity).getSaturation()));
                    //        dirPieces.add(new sectionLosDirPieces("2", calculateLOS(downDensity), calculateSaturation(upDensity).getSaturation()));



                            String sectionName = "路段";
                            if (endStake != null && startStake != null && !startStake.isEmpty() && !endStake.isEmpty()) {
                                sectionName = startStake + "-" + endStake;
                            }
                            System.out.println("===================================");
                            if(section.getEndStake()==1030)resultList.add(new sectionLos("鄂北-大新", "K1016+020-K1030+448", dirPieces));
                            else resultList.add(new sectionLos("大新-大悟", "K1030+448-K1043+400", dirPieces));
                          shangxing1 = 0;
                         xiaxing2 = 0;
                         shangxingSum = 0;
                         xiaxingSum = 0;
                         upkeche = 0;uphuoche = 0;upweihuaping = 0;upzhongxinghuoche = 0;
                         downkeche = 0;downhuoche = 0;downweihuaping = 0;downzhongxinghuoche = 0;


                }//季度表
            }//路段表
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
private Map<Long, VehicleSeg> mergeVehicleSegs(Map<Long, VehicleSeg> map, List<VehicleSeg> segs) {
    Map<Long, VehicleSeg> m1=new HashMap<>();
    for (VehicleSeg seg : segs) {
        VehicleSeg existing = map.get(seg.getCarId());
        if (existing == null) {
            m1.put(seg.getCarId(), seg);
        } else {
            // 合并逻辑（根据业务需求）
            existing.setPointSum(existing.getPointSum() + seg.getPointSum());
            existing.setSpeedSum(existing.getSpeedSum() + seg.getSpeedSum());
            existing.setAverageSpeed(existing.getSpeedSum() / existing.getPointSum());
            m1.put(seg.getCarId(), existing);
        }
    }
    return m1;
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
    // 初始化累加器
    Map<String, Double> znAccumulator = new HashMap<>(); // 路段车辆数累加器
    Map<String, Double> lenAccumulator = new HashMap<>(); // 路段长度累加器
    znAccumulator.put("A", 0.0);
    znAccumulator.put("B", 0.0);
    znAccumulator.put("C", 0.0);
    znAccumulator.put("D", 0.0);

    lenAccumulator.put("A", 0.0);
    lenAccumulator.put("B", 0.0);
    lenAccumulator.put("C", 0.0);
    lenAccumulator.put("D", 0.0);

    long st = convertToTimestamp(beginTime) / 1000 * 1000;
    long tt = convertToTimestamp(endTime) / 1000 * 1000 + 1000;

    // 处理时间范围
    st = st / 60000 * 60000;
    tt = tt / 60000 * 60000 + 60000;

    if (st > tt) {
        long temp = st;
        st = tt;
        tt = temp;
    }

    Map<String, HBaseTableScanner.KeyRange> scanPlan = HBaseTableScanner.generateScanPlan(st, tt);

    // 遍历所有表
    for (Map.Entry<String, HBaseTableScanner.KeyRange> entry1 : scanPlan.entrySet()) {
        String tableName = entry1.getKey();
        String startKey = entry1.getValue().getStartKey();
        String endKey = entry1.getValue().getEndKey();

        // 处理A路段
        String startSK = LocationOP.GETLonNearest(114.03852081298828, roadAKDataList).getLocation();
        String endSK = LocationOP.GETLonNearest(114.04580688476562, roadAKDataList).getLocation();
        int index = startSK.indexOf("+");
        String zaStartMil = (index != -1) ? startSK.substring(2, index) : startSK;
        index = endSK.indexOf("+");
        String zaEndMil = (index != -1) ? endSK.substring(2, index) : endSK;

        int startM = Integer.parseInt(zaStartMil);
        int endM = Integer.parseInt(zaEndMil);
        if (startM > endM) {
            int temp = endM;
            endM = startM;
            startM = temp;
        }
        endM += 1;

        List<VehicleSeg> l = totalOps.getVehicleSegmentsByakRange(tableName,
                Long.parseLong(startKey), Long.parseLong(endKey), startM, endM);

        // 累加A路段数据
        znAccumulator.put("A", znAccumulator.get("A") + l.size());
        lenAccumulator.put("A", (endM - startM) * 4.0);

        // 处理B路段
        startSK = LocationOP.GETLonNearest(114.03852081298828, roadBKDataList).getLocation();
        endSK = LocationOP.GETLonNearest(114.04602813720703, roadBKDataList).getLocation();
        index = startSK.indexOf("+");
        zaStartMil = (index != -1) ? startSK.substring(2, index) : startSK;
        index = endSK.indexOf("+");
        zaEndMil = (index != -1) ? endSK.substring(2, index) : endSK;

        startM = Integer.parseInt(zaStartMil);
        endM = Integer.parseInt(zaEndMil);
        if (startM > endM) {
            int temp = endM;
            endM = startM;
            startM = temp;
        }
        endM += 1;

        l = totalOps.getVehicleSegmentsBybkRange(tableName,
                Long.parseLong(startKey), Long.parseLong(endKey), startM, endM);

        // 累加B路段数据
        znAccumulator.put("B", znAccumulator.get("B") + l.size());
        lenAccumulator.put("B", (endM - startM) * 4.0);

        // 处理C路段
        startSK = LocationOP.GETLonNearest(114.0416030883789, roadCKDataList).getLocation();
        endSK = LocationOP.GETLonNearest(114.04431915283203, roadCKDataList).getLocation();
        index = startSK.indexOf("+");
        zaStartMil = (index != -1) ? startSK.substring(2, index) : startSK;
        index = endSK.indexOf("+");
        zaEndMil = (index != -1) ? endSK.substring(2, index) : endSK;

        startM = Integer.parseInt(zaStartMil);
        endM = Integer.parseInt(zaEndMil);
        if (startM > endM) {
            int temp = endM;
            endM = startM;
            startM = temp;
        }
        endM += 1;

        l = totalOps.getVehicleSegmentsByckRange(tableName,
                Long.parseLong(startKey), Long.parseLong(endKey), startM, endM);

        // 累加C路段数据
        znAccumulator.put("C", znAccumulator.get("C") + l.size());
        lenAccumulator.put("C", (endM - startM) * 4.0);

        // 处理D路段
        startSK = LocationOP.GETLonNearest(114.0438003540039, roadDKDataList).getLocation();
        endSK = LocationOP.GETLonNearest(114.04520416259766, roadDKDataList).getLocation();
        index = startSK.indexOf("+");
        zaStartMil = (index != -1) ? startSK.substring(2, index) : startSK;
        index = endSK.indexOf("+");
        zaEndMil = (index != -1) ? endSK.substring(2, index) : endSK;

        startM = Integer.parseInt(zaStartMil);
        endM = Integer.parseInt(zaEndMil);
        if (startM > endM) {
            int temp = endM;
            endM = startM;
            startM = temp;
        }
        endM += 1;

        l = totalOps.getVehicleSegmentsBydkRange(tableName,
                Long.parseLong(startKey), Long.parseLong(endKey), startM, endM);

        // 累加D路段数据
        znAccumulator.put("D", znAccumulator.get("D") + l.size());
        lenAccumulator.put("D", (endM - startM) * 4.0);
    }

    // 所有表处理完成后，计算LOS
    List<sectionLosDirPieces> sls = new ArrayList<>();

    // 计算A路段的LOS
    double densityA = znAccumulator.get("A") / lenAccumulator.get("A");
    sls.add(new sectionLosDirPieces("A", calculateLOS(densityA), densityA));

    // 计算B路段的LOS
    double densityB = znAccumulator.get("B") / lenAccumulator.get("B");
    sls.add(new sectionLosDirPieces("B", calculateLOS(densityB), densityB));

    // 计算C路段的LOS
    double densityC = znAccumulator.get("C") / lenAccumulator.get("C");
    sls.add(new sectionLosDirPieces("C", calculateLOS(densityC), densityC));

    // 计算D路段的LOS
    double densityD = znAccumulator.get("D") / lenAccumulator.get("D");
    sls.add(new sectionLosDirPieces("D", calculateLOS(densityD), densityD));

    List<sectionLos> slsList = new ArrayList<>();
    slsList.add(new sectionLos("大悟南枢纽", "K1062+700", sls));

    return new sectionLosResult(200, "查找固定桩号成功", slsList, true);
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
    public flowNoNew getFlowNo(String startTime, String endTime, String startStake, String endStake) throws IOException {
        List<totalOpsv3.PeriodTrafficCounts> cl=new ArrayList<>();

        List<String> sections1 = getPassedSections(startStake, endStake);
        for(String section : sections1) {
            System.out.println(section);
            String stStake="";
            String edStake="";
            for(HighwaySectionUtils.HighwaySection section1:SECTIONS){
                if(section1.getStartName().equals(section.substring(0,2))){
                    stStake=section1.getStartStake();
                    edStake=section1.getEndStake();
                    totalOpsv3.PeriodTrafficCounts counts = totalOpsv3.getTrafficCountWithPreviousPeriod(startTime, endTime, stStake, edStake, section);
                    counts.setStake(stStake+"-"+edStake);

                    cl.add(counts);
                }
            }
        }
        List<Integer>list=new ArrayList<>();


       for (totalOpsv3.PeriodTrafficCounts periodTrafficCounts : cl) {
           list.add(periodTrafficCounts.getCurrentDirection1() + periodTrafficCounts.getCurrentDirection2());
       }
        List<Integer>no=calculateRanks(list);
        List<flowNoNew.rank>rnk=new ArrayList<>();
         List<flowNoNew.DirectionInfo> tp0rank=new ArrayList<>();
            List<flowNoNew.DirectionInfo> tp2rank=new ArrayList<>();
            List<flowNoNew.DirectionInfo> tp1rank=new ArrayList<>();
          for(int i = 0 ; i < cl.size(); i++){

            totalOpsv3.PeriodTrafficCounts c=cl.get(no.get(i));


            String mom1="0";
            String mom2="0";
            String mom0="0";
            System.out.println("c.getCurrentDirection1:"+c.getCurrentDirection1()+" c.getCurrentDirection2:"+c.getCurrentDirection2()+"  c.getPreviousDirection1:"+c.getPreviousDirection1()+"  c.getPreviousDirection2:"+c.getPreviousDirection2());
            if(c.getPreviousDirection1()!=0) mom1=(c.getCurrentDirection1()-c.getPreviousDirection1())/c.getPreviousDirection1()+"%";
            if(c.getPreviousDirection2()!=0) mom2=(c.getCurrentDirection2()-c.getPreviousDirection2())/c.getPreviousDirection2()+"%";
            if(c.getPreviousDirection2()+c.getPreviousDirection1()!=0) mom0=(c.getCurrentDirection2()+c.getCurrentDirection1()-c.getPreviousDirection1()-c.getPreviousDirection2())/(c.getPreviousDirection2()+c.getPreviousDirection1())+"%";

            tp0rank.add(new flowNoNew.DirectionInfo(c.getName(),c.getStake(),c.getCurrentDirection1()+c.getCurrentDirection2(),0+"%",mom0));
            tp1rank.add(new flowNoNew.DirectionInfo(c.getName(),c.getStake(),c.getCurrentDirection1(),0+"%",mom1));
            tp2rank.add(new flowNoNew.DirectionInfo(c.getName(),c.getStake(),c.getCurrentDirection2(),0+"%",mom2));

          }
          rnk.add(new flowNoNew.rank(0,tp0rank));
          rnk.add(new flowNoNew.rank(1,tp1rank));
          rnk.add(new flowNoNew.rank(2,tp2rank));


        return new flowNoNew(200, "成功", rnk, true);
    }


  public static List<Integer> calculateRanks(List<Integer> list) {
        // 创建一个索引列表，用于记录原始位置
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            indices.add(i);
        }

        // 根据数值降序排序索引（数值大的排名靠前）
        // 如果数值相同，保持原始顺序
        indices.sort((a, b) -> {
            int valueCompare = Integer.compare(list.get(b), list.get(a)); // 改为降序
            if (valueCompare == 0) {
                return Integer.compare(a, b); // 保持原始顺序
            }
            return valueCompare;
        });

        // 创建结果列表，初始值为0
        List<Integer> result = new ArrayList<>(Collections.nCopies(list.size(), 0));

        // 分配排名
        for (int rank = 0; rank < indices.size(); rank++) {
            int originalIndex = indices.get(rank);
            result.set(originalIndex, rank);
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
        @Override
    public ZaFlowNoResult getZaFlowNo(String startTime, String endTime, String startStake, String endStake) throws IOException {
                TollStationFlowCalculator calculator = new TollStationFlowCalculator();

            TollStationFlowCalculator.TollStationFlow flow = calculator.calculateFlow("孝感收费站", startTime, endTime);
            System.out.println(flow.toJSON().toJSONString());

            // 批量计算多个收费站
            List<String> stations = Arrays.asList("孝感收费站");
            Map<String, TollStationFlowCalculator.TollStationFlow> batchResults = calculator.calculateBatchFlow(stations, startTime, endTime);

        return new ZaFlowNoResult(200, "成功", batchResults ,true);
    }

}




