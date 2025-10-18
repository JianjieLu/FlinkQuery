package com.ljj.flinkquery.demos.web.service;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import com.ljj.flinkquery.FlinkQueryApplication;
import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.entity.VehicleSeg;
import com.ljj.flinkquery.demos.web.impl.edu.hbaseTool;
import com.ljj.flinkquery.demos.web.impl.edu.tools.HBaseTableScanner;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.ljj.flinkquery.demos.entity.TrafficEventUtils.*;

import com.ljj.flinkquery.demos.web.impl.edu.tableOps.*;
import com.ljj.flinkquery.demos.entity.stakeEnvents.*;
import com.ljj.flinkquery.demos.entity.GeoUtils.*;


import static com.ljj.flinkquery.FlinkQueryApplication.*;
import static com.ljj.flinkquery.demos.entity.data.Utils.convertFromTimestampMillis;
import static com.ljj.flinkquery.demos.web.impl.edu.querys.TrafficStatsQuery.aggregateDailyStats;
import static com.ljj.flinkquery.demos.web.impl.edu.querys.TrafficStatsQuery.queryTrafficStats;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.TrafficDataAggregator.aggregateDaily;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.TrafficDataAggregator.aggregateHourly;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.*;
import static java.lang.Math.abs;
import static org.apache.commons.lang3.StringUtils.substring;

//import static com.ljj.flinkquery.FlinkQueryApplication.redisTemplate;
@Service
public class HBaseServiceImpl implements HBaseService {
public static int cnum;
    //ds:HBASE查询优化与性能优化
    // 批量查询方法
//private Map<Long, VehicleSeg> getVehiclesBatch(String tableName, List<String> rowkeys) {
//    Map<Long, VehicleSeg> resultMap = new ConcurrentHashMap<>();
//
//    // 定义列族和列名的字节数组
//    byte[] CF = Bytes.toBytes("cf");
//    byte[] COL_VEHICLE = Bytes.toBytes("VehicleSegments");
//
//    try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
//        // 分页处理（每100个key一批）
//        Lists.partition(rowkeys, 100).forEach(batch -> {
//            List<Get> gets = batch.stream()
//                .map(key -> {
//                    Get get = new Get(Bytes.toBytes(key));
//                    get.addColumn(CF, COL_VEHICLE);  // 在map内部添加列
//                    return get;
//                })
//                .collect(Collectors.toList());
//
//            try {
//                Result[] results = table.get(gets);
//                for (Result result : results) {
//                    byte[] valueBytes = result.getValue(CF, COL_VEHICLE);
//                    if (valueBytes != null) {
//                        parseVehicleSegments(valueBytes).forEach(vs ->
//                            resultMap.merge(vs.getCarId(), vs, this::mergeVehicleSeg)
//                        );
//                    }
//                }
//            } catch (IOException e) {
//                // 处理查询错误
//                System.err.println("批量查询失败: " + e.getMessage());
//            }
//        });
//    } catch (IOException e) {
//        // 错误处理
//        System.err.println("获取表失败: " + e.getMessage());
//    }
//    return resultMap;
//}
//
//// 解析VehicleSeg列表
//private List<VehicleSeg> parseVehicleSegments(byte[] valueBytes) {
//    String json = Bytes.toString(valueBytes);
//    return JSON.parseArray(json, VehicleSeg.class);
//}
//
//// 合并车辆分段数据
//private VehicleSeg mergeVehicleSeg(VehicleSeg existing, VehicleSeg newSeg) {
//    existing.setPointSum(existing.getPointSum() + newSeg.getPointSum());
//    existing.setSpeedSum(existing.getSpeedSum() + newSeg.getSpeedSum());
//    return existing;
//}
//
//// 类静态变量
//private static final byte[] CF = Bytes.toBytes("cf");
//private static final byte[] COL_VEHICLE = Bytes.toBytes("VehicleSegments");
//private static final String ZK_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";
//private static Connection hbaseConnection;
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Autowired
    private RedisTemplate<String, String> redisTemplate1;

    static {
        try {
            roadAKDataList = JsonReader.readJsonFile("AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("DK_locations.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //http://localhost:8080/createTable?tableName=vehicle_data&columnFamilies=info,status
    @Override
    public void createTable(String tableName, List<String> columnFamilies) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        totalOps.createTable(conf, tableName, columnFamilies.get(0));
        if (columnFamilies.size() > 1) {
            for (int i = 1; i < columnFamilies.size(); i++) {
                totalOps.adadColumnFamily(conf, tableName, columnFamilies.get(i));
            }
        }
    }

    @Override
    public void getByRowKey(String tableName, String rowKey)
            throws IOException {
        totalOps.getByRowkey(tableName, rowKey);

    }

    @Override
    public void getEmpty(String tableName, String rowKey) {

    }

    @Override

    public TimeSpatialResult getByTimeSpatialWithID(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {


        return new TimeSpatialResult();
    }
// 新增LOS计算辅助方法
private String calculateLOS(double density) {
    if (density <= 7) return "A";
    else if (density <= 11) return "B";
    else if (density <= 16) return "C";
    else if (density <= 22) return "D";
    else if (density <= 28) return "E";
    else return "F";
}
    public TimeSpatialResult getRedis(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {
        long t1 = System.currentTimeMillis();
        //region Description
        if (startMileage != null) startMileage = startMileage.replace(" ", "+");
        if (endMileage != null) endMileage = endMileage.replace(" ", "+");
        int shangxing1 = 0;
        int xiaxing2 = 0;//下行车辆数
        double shangxingSum = 0;
        double xiaxingSum = 0;//下行平均速度总和
        String startMi = "";
        String endMi = "";//桩号（完整版）
        int startM = 0;
        int endM = 0;//桩号中的数字
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
        String zadaoInfo = "匝道查找信息：";
        int zdeltam = 0;
        int zalen = 0;
        long st = startTime / 1000 * 1000;
        long tt = endTime / 1000 * 1000 + 1000;
        String zaStartMil = "";
        String zaEndMil = "";
        TimeSpatialData kong = new TimeSpatialData(0, 0, 0,0,0,0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "0","0","0",0);
        TimeSpatialResult t2 = new TimeSpatialResult(200, "————无范围内数据,原因：桩号转换失败", kong, true);
        TimeSpatialData tos = new TimeSpatialData();
        boolean za = true;
        boolean main = true;
        StakeAssignment stakeAssign;
        int index;
        int index1;
        String startMil;
        String endMil;
        double n = 0;
        double sum = 0;
        double zn = 0;
        double zsum = 0;
        Map<Long, VehicleSeg> mergedMap = new HashMap<>();
        Map<Long, VehicleSeg> m = new HashMap<>();
        Map<Long, VehicleSeg> am = new HashMap<>();
        Map<Long, VehicleSeg> bm = new HashMap<>();
        Map<Long, VehicleSeg> cm = new HashMap<>();
        Map<Long, VehicleSeg> dm = new HashMap<>();
        boolean b = Objects.equals(startMileage, "") && Objects.equals(endMileage, "");
//        System.out.println("开始内存查找：http://100.65.38.139:8080/getByTimeSpatial?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
        if (Latitude1==2) {
            Set<String> keys = redisTemplate.keys("v2*");
            System.out.println("keys: " + keys);
        }
        if(Latitude2==2){
            Set<String> keys = redisTemplate.keys("v60*");
            System.out.println("keys: " + keys);
        }
        if (b) {
            try {
                stakeAssign = new StakeAssignment("/home/ljj/sx_json.json");
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            startMi = stakeAssign.findInsertionIndex(Longitude1, Latitude1);
            endMi = stakeAssign.findInsertionIndex(Longitude2, Latitude2);
            if (startMi == null || endMi == null) {
                return t2;
            }
        } else {
            startMi = startMileage;
            endMi = endMileage;
        }
        index = startMi.indexOf("+");
        startMil = (index != -1) ? startMi.substring(0, index) : startMi;
        index1 = endMi.indexOf("+");
        endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;
//        if(MBR.hasIntersection(new MBR(114.03852081298828,114.04580688476562,30.91611099243164,30.919893264770508),new MBR(114.03852081298828,114.04602813720703,30.917593002319336,30.920652389526367)))
//        if(MBR.hasIntersection(new MBR(114.0416030883789,114.04431915283203,30.919200897216797,30.92119598388672),new MBR(114.0438003540039,114.04520416259766,30.91492462158203,30.917877197265625)))
        if (Longitude1 > Longitude2) {
            double dou = Longitude2;
            Longitude2 = Longitude1;
            Longitude1 = dou;
        }
        if (Latitude1 > Latitude2) {
            double dou = Latitude2;
            Latitude2 = Latitude1;
            Latitude1 = dou;
        }
        //AK
        if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04580688476562, 30.91611099243164, 30.919893264770508), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
            String startSK = LocationOP.GETLonNearest(Longitude1, roadAKDataList).getLocation();
            String endSK = LocationOP.GETLonNearest(Longitude2, roadAKDataList).getLocation();
            index = startSK.indexOf("+");
            zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
            index1 = endSK.indexOf("+");
            zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("AK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);//zaStartMil:  0  zaEndMil:  0
//                System.out.println("AK  startSK:  " + startSK + "  endSK:  " + endSK);// startSK:  BK0+373.5  endSK:  BK0+688
            //从起始时间到终止时间
            startM = Integer.parseInt(zaStartMil);
            endM = Integer.parseInt(zaEndMil);
            if (startM > endM) {
                int temp = endM;
                endM = startM;
                startM = temp;
            }
            endM += 1;
            zdeltam += endM - startM;
            zadaoInfo = zadaoInfo + "AK" + startM + "to AK" + endM + "," + st + " to " + tt + "  ";
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v60_" + i + "_AK" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
            zalen += 978;
        }
        //BK
        if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04602813720703, 30.917593002319336, 30.920652389526367), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
            String startSK = LocationOP.GETLonNearest(Longitude1, roadBKDataList).getLocation();
            String endSK = LocationOP.GETLonNearest(Longitude2, roadBKDataList).getLocation();
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
            zdeltam += endM - startM;
            zadaoInfo = zadaoInfo + "BK" + startM + "to BK" + endM + "," + st + " to " + tt + "  ";
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v60_" + i + "_BK" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
            zalen += 1085;
        }
        //CK
        if (MBR.hasIntersection(new MBR(114.0416030883789, 114.04431915283203, 30.919200897216797, 30.92119598388672), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
            String startSK = LocationOP.GETLonNearest(Longitude1, roadCKDataList).getLocation();
            String endSK = LocationOP.GETLonNearest(Longitude2, roadCKDataList).getLocation();
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
            zdeltam += endM - startM;
            zadaoInfo = zadaoInfo + "CK" + startM + "to CK" + endM + "," + st + " to " + tt + "  ";
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v60_" + i + "_CK" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
            zalen += 389;
        }
        //DK
        if (MBR.hasIntersection(new MBR(114.0438003540039, 114.04520416259766, 30.91492462158203, 30.917877197265625), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
            String startSK = LocationOP.GETLonNearest(Longitude1, roadDKDataList).getLocation();
            String endSK = LocationOP.GETLonNearest(Longitude2, roadDKDataList).getLocation();
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
            zdeltam += endM - startM;
            zadaoInfo = zadaoInfo + "DK" + startM + "to DK" + endM + "," + st + " to " + tt + "  ";
            for (long i = st; i < tt; i += 1000) {
                for (int j = startM; j < endM; j++) {
                    String redisKey = "v60_" + i + "_DK" + j;
                    List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
            zalen += 366;
        }
        mergedMap.putAll(am);
        mergedMap.putAll(bm);
        mergedMap.putAll(cm);
        mergedMap.putAll(dm);
        mergedMap.forEach((k, v) -> {
            v.setAverageSpeed((int) (v.getSpeedSum() / v.getPointSum()));
        });
        for (Map.Entry<Long, VehicleSeg> entry : mergedMap.entrySet()) {
            VehicleSeg v = entry.getValue();
            if (v != null) {
                zn++;
                zsum += v.getAverageSpeed();
                Integer vt = v.getOriginalType();

                Integer vet =v.getVehicleType();
                if (vt != null) {
                    if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                    else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                        zuphuoche++;
                    else if (vt == 8) {
                        zupweihuaping++;
                        zuphuoche++;
                    }
                }else{
                       if (vet>=1&&vet<=4) zupkeche++;
                    else if (vet >= 11 && vet<= 16)
                        zuphuoche++;
                    else {
                           n--;zsum -= v.getAverageSpeed();
                       }
                }

                if (v.getSpecialFlag() != null) {
                    String[] sd = v.getSpecialFlag().split(";");
                    for (String s : sd)
                        if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                            zupzhongxinghuoche++;
                }

            }
        }

        if (zn != 0) {
            double chemidu = Math.round(((double) mergedMap.size() / zdeltam * 100.0)) / 100.0;
            double busval = Math.round((double) zupkeche / (zuphuoche+zupkeche) * 100.0) / 100.0;
            double trackval = 1-busval;
            tos.setZaAverageSpeed(Math.round((zsum / zn * 100.0)) / 100.0);
            tos.setZaCount(zn);
//            tos.setZaTrafficSaturation(Math.round(zn / (zalen / 1000 + 1) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
            tos.setZaVehicleDensity(chemidu);
            tos.setZaCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
            tos.setZaBusCount(zupkeche);
            tos.setZaTrackCount(zuphuoche);
            tos.setZaChemicalCount(zupweihuaping);
            tos.setZaHeavyTrackCount(zupzhongxinghuoche);
            tos.setZaBusVal(busval);
            tos.setZaTrackVal(trackval);
        } else {
            za = false;
        }

        if (startMil.indexOf("K") != 0) {//非主路
            System.out.println("kong");
        } else {
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
                    String redisKey = "v60_" + i + "_K" + j;
//                    System.out.println(redisKey);
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
        }
        m.forEach((k, v) -> {
            v.setAverageSpeed((int) (v.getSpeedSum() / v.getPointSum()));
        });
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
                    }else{
                       if (vet>=1&&vet<=4) upkeche++;
                    else if (vet >= 11 && vet<= 16)
                        uphuoche++;else {n--;shangxing1--;shangxingSum-= v.getAverageSpeed();}
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
                        downhuoche++;else {n--;xiaxing2--;
                    xiaxingSum -= v.getAverageSpeed();
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
if (n > 0 && (endM - startM) > 0) {
    double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
    double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度

    tos.setMainUpLOS(calculateLOS(upDensity));
    tos.setMainDownLOS(calculateLOS(downDensity));
}

//            System.out.println("timestamp: from " + st + "(" + startTime + ") to " + tt + "(" + endTime + ")  SkateID: from " + startMi + "(" + startMileage + ") to " + endMi + "(" + endMileage + ")");
        if (n != 0) {
            double chemidu = Math.round(((double) m.size() / (endM - startM) * 100.0)) / 100.0;
       double upbval = Math.round((double) (upkeche) / (uphuoche+upkeche) * 100.0) / 100.0;
            double downbval = Math.round((double) (downkeche) / (downhuoche+downkeche) * 100.0) / 100.0;
            double uptval = 1-upbval;
            double downtval = 1-downbval;
            double bval = Math.round((double) (upkeche+downkeche) / (uphuoche+upkeche+downhuoche+downkeche) * 100.0) / 100.0;
            double tval = 1-bval;
            tos.setUpAverageSpeed(Math.round((shangxingSum / shangxing1 * 100.0)) / 100.0);
            tos.setDownAverageSpeed(Math.round(xiaxingSum / xiaxing2 * 100.0) / 100.0);
                tos.setTotalAverageSpeed(Math.round((tos.getUpAverageSpeed()+tos.getDownAverageSpeed())/2* 100.0)/ 100.0);
            tos.setTotalCount((int) n);
            tos.setUpCount(shangxing1);
            tos.setDownCount(xiaxing2);
//            tos.setTrafficSaturation(Math.round(n / ((double) (endM - startM)) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
            tos.setVehicleDensity(chemidu);
            tos.setTotalCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
            tos.setUpCongestionIndex(Math.round((120 / (shangxingSum / shangxing1)) * 1000.0) / 1000.0);
            tos.setDownCongestionIndex(Math.round((120/(xiaxingSum / xiaxing2)  ) * 1000.0) / 1000.0);
            tos.setUpBusCount(upkeche);
            tos.setUpTrackCount(uphuoche);
            tos.setUpChemicalCount(upweihuaping);
            tos.setUpHeavyTrackCount(upzhongxinghuoche);
            tos.setDownBusCount(downkeche);
            tos.setDownTrackCount(downhuoche);
            tos.setDownChemicalCount(downweihuaping);
            tos.setDownHeavyTrackCount(downzhongxinghuoche);
            tos.setUpTruckVal(uptval);
            tos.setUpBusVal(upbval);
            tos.setDownBusVal(downbval);
            tos.setDownTrackVal(downtval);
            tos.setBusVal(bval);
            tos.setTruckVal(tval);
             double chemidu1 = Math.round(((double) shangxing1 / (endM - startM) *2* 100.0)) / 100.0;
            double chemidu2 = Math.round(((double) xiaxing2 / (endM - startM) *2* 100.0)) / 100.0;
            SaturationResult saturationResult1 = calculateSaturation(chemidu1);
            SaturationResult saturationResult2 = calculateSaturation(chemidu2);
            tos.setUpTrafficSaturation((Math.round(saturationResult1.saturation* 100.0)) / 100.0);
            tos.setDownTrafficSaturation(Math.round((saturationResult2.saturation* 100.0)) / 100.0);
        } else main = false;
        System.out.println("n:"+n+" upkeche:"+upkeche+" uphuoche:"+uphuoche+" upweihuaping:"+upweihuaping);
        //endregion
            SaturationResult saturationResult = calculateSaturation(tos.getVehicleDensity());


        //region Description
        if (main && za) {
                tos.setMainLOS(saturationResult.level);

            tos.setZaTrafficSaturation( zaSau(2200*(tt - st) / 3600000.0*((double) zalen /1000),zn));
            // 四舍五入保留两位小数
//            tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);
            tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);
            return new TimeSpatialResult(200, "内存查找————匝道、主路均有数据    查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 1000, tos, true);
        } else if (main && !za){
                tos.setMainLOS(saturationResult.level);

            tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);

            return new TimeSpatialResult(200, "内存查找————主路有数据,匝道无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 1000, tos, true);
        }
        else if (!main && za) {
            tos.setZaTrafficSaturation( zaSau(2200*(tt - st) / 3600000.0*((double) zalen /1000),zn));

            return new TimeSpatialResult(200, "内存查找————主路无数据,匝道有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo + "   查询时段数：" + (tt - st) / 1000, tos, true);
        } else
            return new TimeSpatialResult(200, "内存查找————主路、匝道均无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
    }
  public static double ms(double chengzai,double liuliang){
        return liuliang/chengzai;
    }

        public static class SaturationResult {
        private final String level;
        private final double saturation;

        public SaturationResult(String level, double saturation) {
            this.level = level;
            this.saturation = saturation;
        }

        public String getLevel() {
            return level;
        }

        public double getSaturation() {
            return saturation;
        }

        @Override
        public String toString() {
            return "Level: " + level + ", Saturation: " + String.format("%.2f", saturation);
        }
    }

    /**
     * 根据密度计算饱和度和等级
     *
     * @param density 密度值
     * @return SaturationResult 包含等级和饱和度值的对象
     */
    public static SaturationResult calculateSaturation(double density) {
        double saturation;
        String level;

        if (density < 10) {
            // A级：饱和度在0-0.3之间线性变化
            saturation = density / 10 * 0.3;
            level = "A";
        } else if (density < 16) {
            // B级：饱和度在0.3-0.6之间线性变化
            saturation = 0.3 + (density - 10) / (16 - 10) * (0.6 - 0.3);
            level = "B";
        } else if (density < 24) {
            // C级：饱和度在0.5-0.7之间线性变化
            saturation = 0.5 + (density - 16) / (24 - 16) * (0.7 - 0.5);
            level = "C";
        } else if (density < 35) {
            // D级：饱和度在0.7-0.9之间线性变化
            saturation = 0.7 + (density - 24) / (35 - 24) * (0.9 - 0.7);
            level = "D";
        } else if (density < 45) {
            // E级：饱和度在0.9-1.0之间线性变化
            saturation = 0.9 + (density - 35) / (45 - 35) * (1.0 - 0.9);
            level = "E";
        } else {
            // F级：饱和度超过1.0，每增加1个密度单位增加0.05饱和度
            saturation = 1.0 + (density - 45) * 0.05;
            level = "F";
        }

        // 确保饱和度在合理范围内
        saturation = Math.max(0, Math.min(saturation, 2.0)); // 最大饱和度限制为2.0
 // 使用BigDecimal进行四舍五入并保留两位小数
    BigDecimal bd = new BigDecimal(saturation);
    bd = bd.setScale(2, RoundingMode.HALF_UP); // 四舍五入模式
    double roundedSaturation = bd.doubleValue();

    return new SaturationResult(level, roundedSaturation);
    }
    public TimeSpatialResult getNewstRedis(Long current, Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {
        long t1 = System.currentTimeMillis();
        //region Description
        if (startMileage != null) startMileage = startMileage.replace(" ", "+");
        if (endMileage != null) endMileage = endMileage.replace(" ", "+");
        int shangxing1 = 0;
        int xiaxing2 = 0;//下行车辆数
        double shangxingSum = 0;
        double xiaxingSum = 0;//下行平均速度总和
        String startMi = "";
        String endMi = "";//桩号（完整版）
        int startM = 0;
        int endM = 0;//桩号中的数字
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

        String zadaoInfo = "匝道查找信息：";
        int zdeltam = 0;
        int zalen = 0;
        long st = startTime / 1000 * 1000;
        long tt = endTime / 1000 * 1000 + 1000;
        String zaStartMil = "";
        String zaEndMil = "";
        TimeSpatialData kong = new TimeSpatialData(0, 0, 0,0,0, 0,0,0,0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,"0","0","0",0);
        TimeSpatialResult t2 = new TimeSpatialResult(200, "实时查找————无范围内数据,原因：桩号转换失败", kong, true);
        TimeSpatialData tos = new TimeSpatialData();
        boolean za = true;
        boolean main = true;
        tos.setTodayTotal(getTodayTotalMemory(1L).getKey());

        StakeAssignment stakeAssign;
        int index;
        int index1;
        String startMil;
        String endMil;
        double n = 0;
        double sum = 0;
        double zn = 0;
        double zsum = 0;
        Map<Long, VehicleSeg> mergedMap = new HashMap<>();
        Map<Long, VehicleSeg> m = new HashMap<>();
        Map<Long, VehicleSeg> am = new HashMap<>();
        Map<Long, VehicleSeg> bm = new HashMap<>();
        Map<Long, VehicleSeg> cm = new HashMap<>();
        Map<Long, VehicleSeg> dm = new HashMap<>();


        try {

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
            if (keys.isEmpty()) {
                return new TimeSpatialResult(200, "当前无最新数据", kong, true);
            }
            boolean b = Objects.equals(startMileage, "") && Objects.equals(endMileage, "");
            System.out.println("实时查找：http://100.65.38.139:8080/getByTimeSpatial?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
            if (startMileage.equals("123")) {
                System.out.println("keys: " + keys);
            }
            if (startMileage.equals("a")) {
                System.out.println("keys: " + timesA);
            }
            if (startMileage.equals("b")) {
                System.out.println("keys: " + timesB);
            }
            if (startMileage.equals("c")) {
                System.out.println("keys: " + timesC);
            }
            if (startMileage.equals("d")) {
                System.out.println("keys: " + timesD);
            }
            if (startMileage.equals("k")) {
                System.out.println("keys: " + timesK);
            }

            if (b) {
                try {
                    stakeAssign = new StakeAssignment("/home/ljj/sx_json.json");
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                startMi = stakeAssign.findInsertionIndex(Longitude1, Latitude1);
                endMi = stakeAssign.findInsertionIndex(Longitude2, Latitude2);
                if (startMi == null || endMi == null) {
                    return t2;
                }
            } else {
                startMi = startMileage;
                endMi = endMileage;
            }
            index = startMi.indexOf("+");
            startMil = (index != -1) ? startMi.substring(0, index) : startMi;
            index1 = endMi.indexOf("+");
            endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;

//        if(MBR.hasIntersection(new MBR(114.03852081298828,114.04580688476562,30.91611099243164,30.919893264770508),new MBR(114.03852081298828,114.04602813720703,30.917593002319336,30.920652389526367)))
//        if(MBR.hasIntersection(new MBR(114.0416030883789,114.04431915283203,30.919200897216797,30.92119598388672),new MBR(114.0438003540039,114.04520416259766,30.91492462158203,30.917877197265625)))
            if (Longitude1 > Longitude2) {
                double dou = Longitude2;
                Longitude2 = Longitude1;
                Longitude1 = dou;
            }
            if (Latitude1 > Latitude2) {
                double dou = Latitude2;
                Latitude2 = Latitude1;
                Latitude1 = dou;
            }
            //AK
            if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04580688476562, 30.91611099243164, 30.919893264770508), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadAKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadAKDataList).getLocation();

                List<Long> list = new ArrayList<>();
                for (String key : timesA) list.add(Long.parseLong(key) / 1000 * 1000);

                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("AK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);//zaStartMil:  0  zaEndMil:  0
//                System.out.println("AK  startSK:  " + startSK + "  endSK:  " + endSK);// startSK:  BK0+373.5  endSK:  BK0+688
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;
                zdeltam += endM - startM;

                zadaoInfo = zadaoInfo + "AK" + startM + "to AK" + endM + "," + st + " to " + tt + "  ";
                for (long i : list) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v2_" + i + "_AK" + j;
                        List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
                zalen += 978;
            }
            //BK
            if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04602813720703, 30.917593002319336, 30.920652389526367), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadBKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadBKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
//                System.out.println("BK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
//                System.out.println("BK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                List<Long> list = new ArrayList<>();
                for (String key : timesB) list.add(Long.parseLong(key) / 1000 * 1000);
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM += 1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "BK" + startM + "to BK" + endM + "," + st + " to " + tt + "  ";

                for (long i : list) {

                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v2_" + i + "_BK" + j;

                        List<VehicleSeg> l = ge(redisTemplate, redisKey);

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
                zalen += 1085;
            }
            //CK
            if (MBR.hasIntersection(new MBR(114.0416030883789, 114.04431915283203, 30.919200897216797, 30.92119598388672), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadCKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadCKDataList).getLocation();

                List<Long> list = new ArrayList<>();
                for (String key : timesC) list.add(Long.parseLong(key) / 1000 * 1000);

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
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "CK" + startM + "to CK" + endM + "," + st + " to " + tt + "  ";
                for (long i : list) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v2_" + i + "_CK" + j;
                        List<VehicleSeg> l = ge(redisTemplate, redisKey);
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
                zalen += 389;
            }
            //DK
            if (MBR.hasIntersection(new MBR(114.0438003540039, 114.04520416259766, 30.91492462158203, 30.917877197265625), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadDKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadDKDataList).getLocation();

                List<Long> list = new ArrayList<>();
                for (String key : timesD) list.add(Long.parseLong(key) / 1000 * 1000);
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
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "DK" + startM + "to DK" + endM + "," + st + " to " + tt + "  ";
                for (long i : list) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v2_" + i + "_DK" + j;
                        List<VehicleSeg> l = ge(redisTemplate, redisKey);

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
                zalen += 366;
            }

            mergedMap.putAll(am);
            mergedMap.putAll(bm);
            mergedMap.putAll(cm);
            mergedMap.putAll(dm);
            StringBuilder zss= new StringBuilder();
            mergedMap.forEach((k, v) -> {
                v.setAverageSpeed((int) (v.getSpeedSum() / v.getPointSum()));
            });
            for (Map.Entry<Long, VehicleSeg> entry : mergedMap.entrySet()) {
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
                    }else{
                       if (vet>=1&&vet<=4) zupkeche++;
                    else if (vet >= 11 && vet<= 16)
                        zuphuoche++;
                    else {
                        zsum -= v.getAverageSpeed();
                           zn--;
                       }
                }
                    if (v.getSpecialFlag() != null) {
                        String[] sd = v.getSpecialFlag().split(";");
                        for (String s : sd)
                            if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                zupzhongxinghuoche++;
                    }

                }
            }

            if (zn != 0) {
                double chemidu = Math.round(((double) mergedMap.size() / zdeltam * 100.0)) / 100.0;
                double busTrackVal = Math.round((double) zupkeche / zuphuoche * 100.0) / 100.0;
                tos.setZaAverageSpeed(Math.round((zsum / zn * 100.0)) / 100.0);
                tos.setZaCount(zn);
                tos.setZaTrafficSaturation(Math.round(zn / (zalen / 1000 + 1) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setZaVehicleDensity(chemidu);
                tos.setZaCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
                tos.setZaBusCount(zupkeche);
                tos.setZaTrackCount(zuphuoche);
                tos.setZaChemicalCount(zupweihuaping);
                tos.setZaHeavyTrackCount(zupzhongxinghuoche);
                         double busval = Math.round((double) zupkeche / (zuphuoche+zupkeche) * 100.0) / 100.0;
            double trackval =1-busval;

            tos.setZaBusVal(busval);
            tos.setZaTrackVal(trackval);
            } else {
                za = false;
            }

            if (startMil.indexOf("K") != 0) {//非主路
                System.out.println("kong");
            } else {
                //从起始时间到终止时间
                startM = Integer.parseInt(startMil.substring(1));//前四个数字
                endM = Integer.parseInt(endMil.substring(1)) + 1;
                //startMil.substring(1):1054  endMil.substring(1):1048  startM:1054  endM:1049
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                List<Long> list = new ArrayList<>();
                for (String key : timesK) list.add(Long.parseLong(key) / 1000 * 1000);
//                System.out.println("startMil:" + startMil + "   endMil:" + endMil + "  startM:" + startM + "   endm:" + endM + "st:" + st + "tt" + tt);//startMil:K1054   endMil:K1048  startM:1049   endm:1054
//                System.out.println("startM:" + startM + " endM:" + endM + " iiii:" + list);
                for (long i : list) {

//                for (int j = 1016; j < 1175; j++) {
                    for (int j = startM; j < endM; j++) {

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
            }
            m.forEach((k, v) -> {
                v.setAverageSpeed((int) (v.getSpeedSum() / v.getPointSum()));
            });
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
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    downzhongxinghuoche++;
                        }
                    }
                }
            }
            if (n > 0 && (endM - startM) > 0) {
    double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
    double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度

    tos.setMainUpLOS(calculateLOS(upDensity));
    tos.setMainDownLOS(calculateLOS(downDensity));
}

//            System.out.println("timestamp: from " + st + "(" + startTime + ") to " + tt + "(" + endTime + ")  SkateID: from " + startMi + "(" + startMileage + ") to " + endMi + "(" + endMileage + ")");
            if (n != 0) {
            double chemidu = Math.round(((double) m.size() / (endM - startM) * 100.0)) / 100.0;
            double upbval = Math.round((double) (upkeche) / (uphuoche+upkeche) * 100.0) / 100.0;
            double downbval = Math.round((double) (downkeche) / (downhuoche+downkeche) * 100.0) / 100.0;
            double uptval = 1-upbval;
            double downtval = 1-downbval;
            double bval = Math.round((double) (upkeche+downkeche) / (uphuoche+upkeche+downhuoche+downkeche) * 100.0) / 100.0;
            double tval = 1-bval;
            tos.setUpAverageSpeed(Math.round((shangxingSum / shangxing1 * 100.0)) / 100.0);
                tos.setDownAverageSpeed(Math.round(xiaxingSum / xiaxing2 * 100.0) / 100.0);
                tos.setTotalAverageSpeed(Math.round((tos.getUpAverageSpeed()+tos.getDownAverageSpeed())/2* 100.0)/ 100.0);
                tos.setTotalCount((int) n);

                cnum=(int)n;
                tos.setUpCount(shangxing1);
                tos.setDownCount(xiaxing2);
//                tos.setTrafficSaturation(Math.round(n / ((double) (endM - startM)) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setVehicleDensity(chemidu);
                tos.setTotalCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
                tos.setUpCongestionIndex(Math.round((120 / (shangxingSum / shangxing1)) * 1000.0) / 1000.0);
                tos.setDownCongestionIndex(Math.round((120/(xiaxingSum / xiaxing2)  ) * 1000.0) / 1000.0);
                tos.setUpBusCount(upkeche);
                tos.setUpTrackCount(uphuoche);
                tos.setUpChemicalCount(upweihuaping);
                tos.setUpHeavyTrackCount(upzhongxinghuoche);
                tos.setDownBusCount(downkeche);
                tos.setDownTrackCount(downhuoche);
                tos.setDownChemicalCount(downweihuaping);
                tos.setDownHeavyTrackCount(downzhongxinghuoche);
          tos.setUpTruckVal(uptval);
            tos.setUpBusVal(upbval);
            tos.setDownBusVal(downbval);
            tos.setDownTrackVal(downtval);
            tos.setBusVal(bval);
            tos.setTruckVal(tval);
                double chemidu1 = Math.round(((double) shangxing1 / (endM - startM)*2 * 100.0)) / 100.0;
            double chemidu2 = Math.round(((double) xiaxing2 / (endM - startM) *2* 100.0)) / 100.0;
            SaturationResult saturationResult1 = calculateSaturation(chemidu1);
            SaturationResult saturationResult2 = calculateSaturation(chemidu2);
           tos.setUpTrafficSaturation((Math.round(saturationResult1.saturation* 100.0)) / 100.0);
            tos.setDownTrafficSaturation(Math.round((saturationResult2.saturation* 100.0)) / 100.0);
            } else main = false;
            SaturationResult saturationResult = calculateSaturation(tos.getVehicleDensity());
            long timeWindow = (tt - st);          // 时间窗口（毫秒）
            double minutes = timeWindow / 60000.0; // 转换为分钟


        //region Description
        if (main && za) {
                tos.setMainLOS(saturationResult.level);

              tos.setZaTrafficSaturation( zaSau(2200*(tt - st) / 3600000.0*((double) zalen /1000),zn));
            tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);

                return new TimeSpatialResult(200, "实时查找————匝道、主路均有数据    查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 1000, tos, true);
            } else if (main && !za)
            {
                tos.setMainLOS(saturationResult.level);
            tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);

                return new TimeSpatialResult(200, "实时查找————主路有数据,匝道无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 1000, tos, true);
            }
            else if (!main && za) {
              tos.setZaTrafficSaturation( zaSau(2200*(tt - st) / 3600000.0*((double) zalen /1000),zn));

                return new TimeSpatialResult(200, "实时查找————主路无数据,匝道有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo + "   查询时段数：" + (tt - st) / 1000, tos, true);
            } else
                return new TimeSpatialResult(200, "实时查找————主路、匝道均无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TimeSpatialResult getHbase(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {
        //        Set<String> keys = redisTemplate.keys("v*");
//        System.out.println(keys);
        System.out.println("数据库查询");
        long t1 = System.currentTimeMillis();
        //region Description
        if (startMileage != null) startMileage = startMileage.replace(" ", "+");
        if (endMileage != null) endMileage = endMileage.replace(" ", "+");
        int shangxing1 = 0;
        int xiaxing2 = 0;//下行车辆数
        double shangxingSum = 0;
        double xiaxingSum = 0;//下行平均速度总和
        String startMi = "";
        String endMi = "";//桩号（完整版）
        int startM = 0;
        int endM = 0;//桩号中的数字
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

        String zadaoInfo = "匝道查找信息：";
        int zdeltam = 0;
        int zalen = 0;
        long st = startTime / 1000 * 1000;
        long tt = endTime / 1000 * 1000 + 1000;
        String zaStartMil = "";
        String zaEndMil = "";
        TimeSpatialData kong = new TimeSpatialData(0, 0, 0,0,0, 0, 0, 0, 0, 0, 0, 0,0, 0, 0, 0, 0, 0, 0,0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,"0","0","0",0);
        TimeSpatialResult t2 = new TimeSpatialResult(200, "内存查找————无范围内数据,原因：桩号转换失败", kong, true);
        TimeSpatialData tos = new TimeSpatialData();
        boolean za = true;
        boolean main = true;
        StakeAssignment stakeAssign;
        int index;
        int index1;
        String startMil;
        String endMil;
        double n = 0;
        double sum = 0;
        double zn = 0;
        double zsum = 0;
        Map<Long, hbaseVe.VehicleSeg> mergedMap = new HashMap<>();
        Map<Long, hbaseVe.VehicleSeg> m = new HashMap<>();
        Map<Long, hbaseVe.VehicleSeg> am = new HashMap<>();
        Map<Long, hbaseVe.VehicleSeg> bm = new HashMap<>();
        Map<Long, hbaseVe.VehicleSeg> cm = new HashMap<>();
        Map<Long, hbaseVe.VehicleSeg> dm = new HashMap<>();
//Set<String> keys = redisTemplate.keys("v*");
//            System.out.println("keys: "+keys);
//        System.out.println(System.currentTimeMillis());
        boolean b = Objects.equals(startMileage, "") && Objects.equals(endMileage, "");
        {
//            System.out.println("开始数据库查找，查找语句：http://100.65.38.139:8080/getByTimeSpatialWithID?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
            shangxing1 = 0;
            xiaxing2 = 0;//下行车辆数
            shangxingSum = 0;
            xiaxingSum = 0;//下行平均速度总和1746857652000
            startMi = "";
            endMi = "";//桩号（完整版）
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


            zdeltam = 0;
            zalen = 0;
            st = startTime / 60000 * 60000;
            tt = endTime / 60000 * 60000 + 60000;
            zaStartMil = "";
            zaEndMil = "";
            kong = new TimeSpatialData(0, 0, 0,0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,"0","0","0",0);
            t2 = new TimeSpatialResult(200, "8001————无范围内数据,原因：桩号转换失败", kong, true);
            tos = new TimeSpatialData();
            za = true;
            main = true;
            zadaoInfo = "匝道查找信息：";
            if (b) {
                try {
                    stakeAssign = new StakeAssignment("/home/ljj/xx_json.json");
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                startMi = stakeAssign.findInsertionIndex(Longitude1, Latitude1);
                endMi = stakeAssign.findInsertionIndex(Longitude2, Latitude2);
                if (startMi == null || endMi == null) {
                    return t2;
                }
            } else {
                startMi = startMileage;
                endMi = endMileage;
            }
            index = startMi.indexOf("+");
            startMil = (index != -1) ? startMi.substring(0, index) : startMi;
            index1 = endMi.indexOf("+");
            endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;

            n = 0;
            sum = 0;
            zn = 0;
            zsum = 0;
//        if(MBR.hasIntersection(new MBR(114.03852081298828,114.04580688476562,30.91611099243164,30.919893264770508),new MBR(114.03852081298828,114.04602813720703,30.917593002319336,30.920652389526367)))
//        if(MBR.hasIntersection(new MBR(114.0416030883789,114.04431915283203,30.919200897216797,30.92119598388672),new MBR(114.0438003540039,114.04520416259766,30.91492462158203,30.917877197265625)))
            if (Longitude1 > Longitude2) {
                double dou = Longitude2;
                Longitude2 = Longitude1;
                Longitude1 = dou;
            }
            if (Latitude1 > Latitude2) {
                double dou = Latitude2;
                Latitude2 = Latitude1;
                Latitude1 = dou;
            }
Map<String, HBaseTableScanner.KeyRange> scanPlan = HBaseTableScanner.generateScanPlan(st, tt);
            //AK
            if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04580688476562, 30.91611099243164, 30.919893264770508), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadAKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadAKDataList).getLocation();
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
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "AK" + startM + "to AK" + endM + "," + st + " to " + tt + "  ";
            for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {

                long qst= Long.parseLong(entry.getValue().getStartKey());

                long qet= Long.parseLong(entry.getValue().getEndKey());
                          List<hbaseVe.VehicleSegAccumulator>lv=totalOps.filterScan(entry.getKey(),qst,qet,startM,endM);

                  for (hbaseVe.VehicleSegAccumulator hba : lv) {

                      List<hbaseVe.VehicleSeg> mylist = new ArrayList<>(hba.getVehicleSegMapD2().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (am.get(vs.getCarId()) == null) {
                                am.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = am.get(vs.getCarId());
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
                zalen += 978;
            }
            //BK
            if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04602813720703, 30.917593002319336, 30.920652389526367), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadBKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadBKDataList).getLocation();
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
                zdeltam += endM - startM;
           for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {

                long qst= Long.parseLong(entry.getValue().getStartKey());
                long qet= Long.parseLong(entry.getValue().getEndKey());
                              List<hbaseVe.VehicleSegAccumulator>lv=totalOps.filterScan(entry.getKey(),qst,qet,startM,endM);

                  for (hbaseVe.VehicleSegAccumulator hba : lv) {

                      List<hbaseVe.VehicleSeg> mylist = new ArrayList<>(hba.getVehicleSegMapD2().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (bm.get(vs.getCarId()) == null) {
                                bm.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = bm.get(vs.getCarId());
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
                zalen += 1085;
            }
            //CK
            if (MBR.hasIntersection(new MBR(114.0416030883789, 114.04431915283203, 30.919200897216797, 30.92119598388672), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadCKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadCKDataList).getLocation();
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
                zdeltam += endM - startM;
                for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {

                long qst= Long.parseLong(entry.getValue().getStartKey());
                long qet= Long.parseLong(entry.getValue().getEndKey());
                                List<hbaseVe.VehicleSegAccumulator>lv=totalOps.filterScan(entry.getKey(),qst,qet,startM,endM);

                  for (hbaseVe.VehicleSegAccumulator hba : lv) {

                      List<hbaseVe.VehicleSeg> mylist = new ArrayList<>(hba.getVehicleSegMapD2().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (cm.get(vs.getCarId()) == null) {
                                cm.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = cm.get(vs.getCarId());
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
                zalen += 389;
            }
            //DK
            if (MBR.hasIntersection(new MBR(114.0438003540039, 114.04520416259766, 30.91492462158203, 30.917877197265625), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadDKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadDKDataList).getLocation();
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
                zdeltam += endM - startM;
                  for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {

                long qst= Long.parseLong(entry.getValue().getStartKey());

                long qet= Long.parseLong(entry.getValue().getEndKey());

                               List<hbaseVe.VehicleSegAccumulator>lv=totalOps.filterScan(entry.getKey(),qst,qet,startM,endM);

                  for (hbaseVe.VehicleSegAccumulator hba : lv) {

                      List<hbaseVe.VehicleSeg> mylist = new ArrayList<>(hba.getVehicleSegMapD2().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (dm.get(vs.getCarId()) == null) {
                                dm.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = dm.get(vs.getCarId());
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
                zalen += 366;
            }

            mergedMap.putAll(am);
            mergedMap.putAll(bm);
            mergedMap.putAll(cm);
            mergedMap.putAll(dm);
            mergedMap.forEach((k, v) -> {
                v.setAveSpeed((float) (v.getSpeedSum() / v.getPointSum()));
            });
            for (Map.Entry<Long, hbaseVe.VehicleSeg> entry : mergedMap.entrySet()) {
                hbaseVe.VehicleSeg v = entry.getValue();
                if (v != null) {
                    zn++;
                    zsum += v.getAveSpeed();

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
                    }else{
                       if (vet>=1&&vet<=4) zupkeche++;
                    else if (vet >= 11 && vet<= 16)
                        zuphuoche++;
                    else {
                           zn--;
                           zsum -= v.getAveSpeed();
                       }
                }
                    if (v.getSpecialFlag() != null) {
                        String[] sd = v.getSpecialFlag().split(";");
                        for (String s : sd)
                            if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                zupzhongxinghuoche++;
                    }

                }
            }

            if (zn != 0) {
                double chemidu = Math.round(((double) mergedMap.size() / zdeltam * 100.0)) / 100.0;
                double busTrackVal = Math.round((double) zupkeche / zuphuoche * 100.0) / 100.0;
                tos.setZaAverageSpeed(Math.round((zsum / zn * 100.0)) / 100.0);
                tos.setZaCount(zn);
                tos.setZaTrafficSaturation(Math.round(zn / (zalen / 1000 + 1) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setZaVehicleDensity(chemidu);
                tos.setZaCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
                tos.setZaBusCount(zupkeche);
                tos.setZaTrackCount(zuphuoche);
                tos.setZaChemicalCount(zupweihuaping);
                tos.setZaHeavyTrackCount(zupzhongxinghuoche);
                         double busval = Math.round((double) zupkeche / (zuphuoche+zupkeche) * 100.0) / 100.0;
            double trackval = 1-busval;

   tos.setZaBusVal(busval);
            tos.setZaTrackVal(trackval);
            } else {
                za = false;
            }

            if (startMil.indexOf("K") != 0) {
                System.out.println("kong");
            } else {
                //从起始时间到终止时间
                startM = Integer.parseInt(startMil.substring(1));//前四个数字
                endM = Integer.parseInt(endMil.substring(1)) + 1;
                System.out.println("startMil.substring(1):" + startMil.substring(1) + "  endMil.substring(1):" + endMil.substring(1) + "  startM:" + startM + "  endM:" + endM);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {

                long qst= Long.parseLong(entry.getValue().getStartKey());
                long qet= Long.parseLong(entry.getValue().getEndKey());
//                List<String> rs = new ArrayList<>();
//                for (long i = qst; i < qet; i += 60000) {
//                    for (int j = startM; j < endM; j++) {
//                        rs.add(i + "_K" + j);
//                    }
//                }
//                    System.out.println("rs:"+rs.toString());
//                    System.out.println("entry.getKey()"+entry.getKey());
                List<hbaseVe.VehicleSegAccumulator>lv=totalOps.filterScan(entry.getKey(),qst,qet,startM,endM);
                  for (hbaseVe.VehicleSegAccumulator hba : lv) {
                      List<hbaseVe.VehicleSeg> mylist = new ArrayList<>(hba.getVehicleSegMapD2().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (m.get(vs.getCarId()) == null) {
                                vs.setDirection(2);
                                m.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = m.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(2);
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                m.put(vs.getCarId(), vs);
                            }
                      }

                       mylist = new ArrayList<>(hba.getVehicleSegMapD1().values());
                      for(hbaseVe.VehicleSeg vs : mylist){
                          //判断是否有重复出
                            if (m.get(vs.getCarId()) == null) {
                                vs.setDirection(1);
                                m.put(vs.getCarId(), vs);
                            } else {
                                hbaseVe.VehicleSeg yuan = m.get(vs.getCarId());
                                vs.setPlateNo(vs.getPlateNo());
                                vs.setDirection(1);
                                vs.setPointSum(yuan.getPointSum() + vs.getPointSum());
                                vs.setSpeedSum(yuan.getSpeedSum() + vs.getSpeedSum());
                                vs.setSpecialFlag(vs.getSpecialFlag());
                                m.put(vs.getCarId(), vs);
                            }
                      }
                        }
            }
            }
            m.forEach((k, v) -> {
                v.setAveSpeed((float) (v.getSpeedSum() / v.getPointSum()));
            });
            for (Map.Entry<Long, hbaseVe.VehicleSeg> entry : m.entrySet()) {
                hbaseVe.VehicleSeg v = entry.getValue();
//                System.out.println(v);
                if (v != null) {
                    n++;
                    sum += v.getAveSpeed();

                    if (v.getDirection() == 1) {
                        shangxing1++;
                        shangxingSum += v.getAveSpeed();

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
                        }
                        else{
                       if (vet>=1&&vet<=4) upkeche++;
                       else if (vet >= 11 && vet<= 16) uphuoche++;
                       else {n--;shangxing1--;shangxingSum -= v.getAveSpeed();}
                }
                        if (v.getSpecialFlag() != null) {
                            String[] sd = v.getSpecialFlag().split(";");
                            for (String s : sd)
                                if (s.equals("20") || s.equals("21") || s.equals("22") || s.equals("23"))
                                    upzhongxinghuoche++;
                        }
                    } else if (v.getDirection() == 2) {
                        xiaxing2++;
                        xiaxingSum += v.getAveSpeed();
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
                    else {n--;xiaxing2--;}
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
            if (n > 0 && (endM - startM) > 0) {
    double upDensity = shangxing1 / ((endM - startM) * 4.0);  // 上行密度（辆/公里/车道）
    double downDensity = xiaxing2 / ((endM - startM) * 4.0); // 下行密度

    tos.setMainUpLOS(calculateLOS(upDensity));
    tos.setMainDownLOS(calculateLOS(downDensity));
}

            System.out.println("timestamp: from " + st + "(" + startTime + ") to " + tt + "(" + endTime + ")  SkateID: from " + startMi + "(" + startMileage + ") to " + endMi + "(" + endMileage + ")");
            if (n != 0) {
                double chemidu = Math.round(((double) m.size() / (endM - startM) * 100.0)) / 100.0;
                double busTrackVal = Math.round((double) (downkeche + upkeche) / (uphuoche + downhuoche) * 100.0) / 100.0;
                double upbusTrackVal = Math.round((double) (upkeche) / (uphuoche) * 100.0) / 100.0;
                double downbusTrackVal = Math.round((double) (downkeche) / (downhuoche) * 100.0) / 100.0;

                tos.setUpAverageSpeed(Math.round((shangxingSum / shangxing1 * 100.0)) / 100.0);
                tos.setDownAverageSpeed(Math.round(xiaxingSum / xiaxing2 * 100.0) / 100.0);
                tos.setTotalAverageSpeed(Math.round((tos.getUpAverageSpeed()+tos.getDownAverageSpeed())/2* 100.0)/ 100.0);
                tos.setTotalCount((int) n);
                tos.setUpCount(shangxing1);
                tos.setDownCount(xiaxing2);
//                tos.setTrafficSaturation(Math.round(n / ((double) (endM - startM)) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setVehicleDensity(chemidu);
                tos.setTotalCongestionIndex(Math.round((120 / (sum / n)) * 1000.0) / 1000.0);
                tos.setUpCongestionIndex(Math.round((120 / (shangxingSum / shangxing1)) * 1000.0) / 1000.0);
                tos.setDownCongestionIndex(Math.round((120/(xiaxingSum / xiaxing2)  ) * 1000.0) / 1000.0);
                tos.setUpBusCount(upkeche);
                tos.setUpTrackCount(uphuoche);
                tos.setUpChemicalCount(upweihuaping);
                tos.setUpHeavyTrackCount(upzhongxinghuoche);
                tos.setDownBusCount(downkeche);
                tos.setDownTrackCount(downhuoche);
                tos.setDownChemicalCount(downweihuaping);
                tos.setDownHeavyTrackCount(downzhongxinghuoche);
        double upbval = Math.round((double) (upkeche) / (uphuoche+upkeche) * 100.0) / 100.0;
            double downbval = Math.round((double) (downkeche) / (downhuoche+downkeche) * 100.0) / 100.0;
            double uptval = 1-upbval;
            double downtval = 1-downbval;
            double bval = Math.round((double) (upkeche+downkeche) / (uphuoche+upkeche+downhuoche+downkeche) * 100.0) / 100.0;
            double tval = 1-bval;
          tos.setUpTruckVal(uptval);
            tos.setUpBusVal(upbval);
            tos.setDownBusVal(downbval);
            tos.setDownTrackVal(downtval);
            tos.setBusVal(bval);
            tos.setTruckVal(tval);
                double chemidu1 = Math.round(((double) shangxing1 / (endM - startM) *2* 100.0)) / 100.0;
            double chemidu2 = Math.round(((double) xiaxing2 / (endM - startM) *2* 100.0)) / 100.0;
            SaturationResult saturationResult1 = calculateSaturation(chemidu1);
            SaturationResult saturationResult2 = calculateSaturation(chemidu2);
            tos.setUpTrafficSaturation((Math.round(saturationResult1.saturation* 100.0)) / 100.0);
            tos.setDownTrafficSaturation(Math.round((saturationResult2.saturation* 100.0)) / 100.0);
            } else {
                main = false;
            }
            long timeWindow = (tt - st);          // 时间窗口（毫秒）
            double minutes = timeWindow / 60000.0; // 转换为分钟
            SaturationResult saturationResult = calculateSaturation(tos.getVehicleDensity());

        
        //region Description
        if (main && za) {
                tos.setMainLOS(saturationResult.level);

                tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);

                return new TimeSpatialResult(200, "数据库查找————匝道、主路均有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 60000, tos, true);
            } else if (main && !za) {
                tos.setMainLOS(saturationResult.level);
                tos.setTrafficSaturation(Math.round(saturationResult.saturation * 100.0) / 100.0);

                return new TimeSpatialResult(200, "数据库查找————主路有数据,匝道无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "   查询主路路段数：" + (endM - startM) + "   查询时段数：" + (tt - st) / 60000, tos, true);
            }else if (!main && za) {
                tos.setZaTrafficSaturation( zaSau(2200*(tt - st) / 3600000.0*((double) zalen /1000),zn));

                return new TimeSpatialResult(200, "数据库查找————主路无数据,匝道有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
            } else
                return new TimeSpatialResult(200, "数据库查找————主路、匝道均无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
        }
    }

    @Override
    public TimeSpatialResult getByTimeSpatial(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2,Integer c) throws IOException {
        long currentTime = System.currentTimeMillis();
        System.out.println("startTime-currentTime:" + abs(startTime - currentTime));
        System.out.println("startTime："+startTime);
        System.out.println("currentTime："+currentTime);
        if(c==2)return getHbase( startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
        else return getNewstRedis(currentTime, startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
//        if (abs(startTime - currentTime) < 20000) {
//            return getNewstRedis(currentTime, startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
//        }
//        if (startTime > timeSplit) {
//            return getNewstRedis(currentTime,startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
//        } else if (endTime > timeSplit & startTime < timeSplit) {
//            return getNewstRedis(currentTime,startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
////            return new TimeSpatialResult(200, "内存、数据库同时查询", mergeRH(getRedis(timeSplit, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2).getData(), getHbase(startTime, timeSplit, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2).getData()), true);
//        } else if (endTime < timeSplit) {
//            return getNewstRedis(startTime,startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
//        }
////        return new TimeSpatialResult(200, "起始时间必须大于终止时间", null, true);
//        return getNewstRedis(startTime,startTime, endTime, startMileage, endMileage, Longitude1, Latitude1, Longitude2, Latitude2);
    }

//    public static TimeSpatialData mergeRH(TimeSpatialData d1, TimeSpatialData d2) {
//        TimeSpatialData merged = new TimeSpatialData();
//
//        // 合并计数类字段（直接相加）
//        merged.setTotalCount(d1.getTotalCount() + d2.getTotalCount());
//        merged.setUpCount(d1.getUpCount() + d2.getUpCount());
//        merged.setDownCount(d1.getDownCount() + d2.getDownCount());
//
//        merged.setUpBusCount(d1.getUpBusCount() + d2.getUpBusCount());
//        merged.setUpTrackCount(d1.getUpTrackCount() + d2.getUpTrackCount());
//        merged.setUpChemicalCount(d1.getUpChemicalCount() + d2.getUpChemicalCount());
//        merged.setUpHeavyTrackCount(d1.getUpHeavyTrackCount() + d2.getUpHeavyTrackCount());
//
//        merged.setDownBusCount(d1.getDownBusCount() + d2.getDownBusCount());
//        merged.setDownTrackCount(d1.getDownTrackCount() + d2.getDownTrackCount());
//        merged.setDownChemicalCount(d1.getDownChemicalCount() + d2.getDownChemicalCount());
//        merged.setDownHeavyTrackCount(d1.getDownHeavyTrackCount() + d2.getDownHeavyTrackCount());
//
//        // 匝道计数类字段（直接相加）
//        merged.setZaBusCount(d1.getZaBusCount() + d2.getZaBusCount());
//        merged.setZaTrackCount(d1.getZaTrackCount() + d2.getZaTrackCount());
//        merged.setZaChemicalCount(d1.getZaChemicalCount() + d2.getZaChemicalCount());
//        merged.setZaHeavyTrackCount(d1.getZaHeavyTrackCount() + d2.getZaHeavyTrackCount());
//
//        // 平均速度（加权平均）
//        merged.setTotalAverageSpeed(calculateWeightedAverage(
//                d1.getTotalAverageSpeed(), d1.getTotalCount(),
//                d2.getTotalAverageSpeed(), d2.getTotalCount()
//        ));
//        merged.setUpAverageSpeed(calculateWeightedAverage(
//                d1.getUpAverageSpeed(), d1.getUpCount(),
//                d2.getUpAverageSpeed(), d2.getUpCount()
//        ));
//        merged.setDownAverageSpeed(calculateWeightedAverage(
//                d1.getDownAverageSpeed(), d1.getDownCount(),
//                d2.getDownAverageSpeed(), d2.getDownCount()
//        ));
//
//        // 匝道平均速度（加权平均）
//        merged.setZaAverageSpeed(calculateWeightedAverage(
//                d1.getZaAverageSpeed(), (int) d1.getZaCount(),
//                d2.getZaAverageSpeed(), (int) d2.getZaCount()
//        ));
//
//        // 交通指标（简单平均，实际需根据业务逻辑调整）
//        merged.setTrafficSaturation((d1.getTrafficSaturation() + d2.getTrafficSaturation()) / 2);
//        merged.setVehicleDensity((d1.getVehicleDensity() + d2.getVehicleDensity()) / 2);
//merged.setUpTrafficSaturation((d1.getUpTrafficSaturation() + d2.getUpTrafficSaturation())/2);
//merged.setDownTrafficSaturation((d1.getDownTrafficSaturation() + d2.getDownTrafficSaturation())/2);
//        // 拥塞指数（简单平均）
//        merged.setTotalCongestionIndex((d1.getTotalCongestionIndex() + d2.getTotalCongestionIndex()) / 2);
//        merged.setUpCongestionIndex((d1.getUpCongestionIndex() + d2.getUpCongestionIndex()) / 2);
//        merged.setDownCongestionIndex((d1.getDownCongestionIndex() + d2.getDownCongestionIndex()) / 2);
//
//        // 客货比（重新计算）
//        merged.setBusTrackVal(calculateRatio(
//                merged.getUpBusCount() + merged.getDownBusCount(),
//                merged.getUpTrackCount() + merged.getDownTrackCount()
//        ));
//        merged.setUpBusTrackVal(calculateRatio(
//                merged.getUpBusCount(),
//                merged.getUpTrackCount()
//        ));
//        merged.setDownBusTrackVal(calculateRatio(
//                merged.getDownBusCount(),
//                merged.getDownTrackCount()
//        ));
//
//        // 匝道交通指标（简单平均）
//        merged.setZaTrafficSaturation((d1.getZaTrafficSaturation() + d2.getZaTrafficSaturation()) / 2);
//        merged.setZaVehicleDensity((d1.getZaVehicleDensity() + d2.getZaVehicleDensity()) / 2);
//        merged.setZaCongestionIndex((d1.getZaCongestionIndex() + d2.getZaCongestionIndex()) / 2);
//        merged.setZaBusTrackVal(calculateRatio(
//                merged.getZaBusCount(),
//                merged.getZaTrackCount()
//        ));
//
//        return merged;
//    }

    // 计算加权平均值
    private static double calculateWeightedAverage(double avg1, int count1, double avg2, int count2) {
        int total = count1 + count2;
        return total > 0 ? (avg1 * count1 + avg2 * count2) / total : 0;
    }

    // 计算比值（防止除以零）
    private static double calculateRatio(int numerator, int denominator) {
        return denominator != 0 ? (double) numerator / denominator : 0;
    }

    //隔一百米
    @Override
    public List<CongestionEvent> getCrowdedInfo(Long startTime, Long endTime, String startMileage, String endMileage) throws IOException {
        int startM = Integer.parseInt(startMileage.substring(1));
        int endM = Integer.parseInt(endMileage.substring(1)) + 1;
        long st = startTime / 60000 * 60000;
        long tt = endTime / 60000 * 60000 + 60000;
        for (long i = st; i < tt; i += 60000) {
            for (int j = startM; j < endM; j++) {
                List<CrowdedInfo> l = totalOps.getCrowdedByRowkey("crowded", i + "_K" + j);
                for (CrowdedInfo vs : l) {

                }
            }
        }
        return null;
    }

    @Override
    public CongestionEvent getCongestionEventById(int eventId) {
        return null;
    }

public static double mainSau(double capacity,double n){

// 防止除零错误
if (capacity < 1) capacity = 1;

// 实际交通量 = 去重后的车辆总数

// 计算饱和度并限制在0-1之间
double trafficSaturation = n / capacity;
trafficSaturation = Math.min(1.0, Math.max(0, trafficSaturation));
return Math.round(trafficSaturation * 100.0) / 100.0;
// 设置结果
}
public static double zaSau(double zaCapacity,double zn){


// 防止除零错误
if (zaCapacity < 1) zaCapacity = 1;



// 计算饱和度并限制在0-1之间
double zaTrafficSaturation = zn / zaCapacity;
zaTrafficSaturation = Math.min(1.0, Math.max(0, zaTrafficSaturation));
return Math.round(zaTrafficSaturation * 100.0) / 100.0;

// 设置结果
}// 匝道交通饱和度计算

    //rowkey:分钟
    //每一个小时存一次，表名为basename+时间+方向


    //表以小时存。
    //用时间查出每个小时按方向的所有list
    //时间小于一小时，查一张表，找出那个时间段哪个路段的所有数据
    //时间大于一小时，查多个表，找出每个表内那个时间段哪个路段的所有数据
//http://100.65.38.139:8080/getCongestionEvent?startTime=1743158735648&endTime=1743158735690&startMileage=&endMileage=&Longitude1=114.04516&Latitude1=30.916416&Longitude2=114.045304&Latitude2=30.916420&direction=1
//http://100.65.38.139:8080/getCongestionEvent?startTime=1743847100000&endTime=1743847200000&startMileage=&endMileage=&Longitude1=114.04516&Latitude1=30.916416&Longitude2=114.045304&Latitude2=30.916420&direction=1
//    http://100.65.38.139:8080/getCongestionEvent?startTime=1743847100000&endTime=1743847200000&startMileage=104800&endMileage=106209&direction=1
    @Override
    public List<CongestionEventResult> getCongestionEvent(Long startTime, Long endTime, Integer startMileage, Integer endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2, int direction) throws IOException {
        List<CongestionEventResult> res = new ArrayList<>();
        long st = startTime / 3600000 * 3600000;
        long tt = endTime / 3600000 * 3600000 + 3600000;
//        long stRowkey= startTime/60000*60000;
//        long ttRowkey= st+300000;
        long temp = st + 3600000;
        List<Long> rows = new ArrayList<>();
        if (startMileage == null || endMileage == null) {
            for (long i = st; i < tt; i += 3600000) {
                for (long j = st; j < temp; j += 60000) {
                    rows.add(j);
                }
//                stRowkey= ttRowkey;
//                ttRowkey+=300000;
                temp += 3600000;
                st += 3600000;

                //时间戳转化为表名,找出按分钟所有的条
                List<Pair<CongestionEvent, Long>> l = totalOps.getCongestionEvent(hbaseTool.convertToCongestionTableName(st, direction), rows);
                for (Pair<CongestionEvent, Long> ce : l) {
                    CongestionEvent c = ce.getKey();
                    //存的时候rowkey里好像已经有方向，再totalops里查出来再高就好了
                    if (ce.getValue() >= startTime && ce.getValue() <= endTime && c.getStartLatitude() <= Latitude1 && c.getStartLongitude() <= Longitude1 && c.getEndLatitude() >= Latitude2 && c.getEndLongitude() >= Longitude2) {
                        res.add(new CongestionEventResult(c, 200, "成功", true));
                    }
                }
                rows.clear();
            }
        } else {
            for (long i = st; i < tt; i += 3600000) {
                for (long j = st; j < temp; j += 60000) {
                    rows.add(j);
                }
//                stRowkey= ttRowkey;
//                ttRowkey+=300000;
                temp += 3600000;
                st += 3600000;
                //时间戳转化为表名,找出按分钟所有的条
                List<Pair<CongestionEvent, Long>> l = totalOps.getCongestionEvent(hbaseTool.convertToCongestionTableName(st, direction), rows);
                for (Pair<CongestionEvent, Long> ce : l) {
                    CongestionEvent c = ce.getKey();
                    //存的时候rowkey里好像已经有方向，再totalops里查出来再高就好了
                    if (ce.getValue() >= startTime && ce.getValue() <= endTime && c.getStartMileage() >= startMileage && c.getEndMileage() <= endMileage) {
                        res.add(new CongestionEventResult(c, 200, "成功", true));
                    }
                }
                rows.clear();
            }
        }

        return res;
    }
public boolean sameLevel(long t1,long t2){
        return t1/3600000*3600000==t2/3600000*3600000;
}
public static boolean sameDay(long timestamp1, long timestamp2) {
        // 默认使用系统时区，也可以指定时区如 ZoneId.of("Asia/Shanghai")
        LocalDate date1 = Instant.ofEpochMilli(timestamp1)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();

        LocalDate date2 = Instant.ofEpochMilli(timestamp2)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();

        return date1.equals(date2);
    }

    @Override
    public SectionalBatchFlowResult getBatchSectionalFlow(List<SectionalFlowQuery> queries) {
        List<SectionalFlowData> list=queries.stream()
            .map(query -> {
                try {
                    return getSingleSectionalFlow(
                        query.getStartTime(),
                        query.getEndTime(),
                        query.getStartMileage(),
                        query.getEndMileage(),
                        query.getLongitude1(),
                        query.getLatitude1(),
                        query.getLongitude2(),
                        query.getLatitude2(),
                        query.getLevel()
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
        return new SectionalBatchFlowResult(200,"查询成功",list,true);
    }
  class Counters {
        int uptotal = 0;
        int downtotal = 0;
        int busUp = 0;
        int trackUp = 0;
        int busDown = 0;
        int trackDown = 0;
    }

    private SectionalFlowData getSingleSectionalFlow(Long startTime, Long endTime,
                                                 String startMileage, String endMileage,
                                                 Double longitude1, Double latitude1,
                                                 Double longitude2, Double latitude2,
                                                 Integer level) throws IOException {
    System.out.println("=======================进入getSingleSectionalFlow====================");
    SectionalFlowQuery inputParams = new SectionalFlowQuery(
            startTime, endTime, startMileage, endMileage,
            longitude1, latitude1, longitude2, latitude2, level
    );
    if (startMileage != null) startMileage = startMileage.replace(" ", "+");
    if (endMileage != null) endMileage = endMileage.replace(" ", "+");

    boolean b = Objects.equals(startMileage, "") && Objects.equals(endMileage, "");
    StakeAssignment stakeAssign;
    String startMi = "";
    String endMi = "";
    int startM = 0;
    int endM = 0;
    int index;
    String startMil = "";
    int index1;

    String endMil = "";

    if (b) {
        try {
            stakeAssign = new StakeAssignment("/home/ljj/xx_json.json");
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        startMi = stakeAssign.findInsertionIndex(longitude1, latitude1);
        endMi = stakeAssign.findInsertionIndex(longitude2, latitude2);
        if (startMi == null || endMi == null) {
            return new SectionalFlowData(null, inputParams);
        }
    } else {
        startMi = startMileage;
        endMi = endMileage;
    }

    index = startMi.indexOf("+");
    startMil = (index != -1) ? startMi.substring(0, index) : startMi;
    index1 = endMi.indexOf("+");
    endMil = (index1 != -1) ? endMi.substring(0, index1) : endMi;

    startM = Integer.parseInt(startMil.substring(1));
    endM = Integer.parseInt(endMil.substring(1)) + 1;

    if (startM > endM) {
        int temp = endM;
        endM = startM;
        startM = temp;
    }
    long thinTime = startTime / 60000 * 60000-60000;
    long endThinTime = endTime / 60000 * 60000;
    System.out.println("sectional flow's startM:" + startM + " endM:" + endM);
Map<String, HBaseTableScanner.KeyRange> scanPlan = HBaseTableScanner.generateScanPlan(startTime, endTime);

  // 在循环外部初始化累加数据结构
List<SectionalFlowPiece> staList1 = new ArrayList<>();
List<SectionalFlowPiece> staList2 = new ArrayList<>();
List<SectionalFlowDat> lsec = new ArrayList<>();
int totalSum1 = 0;
int totalSum2 = 0;
 double factor= (double) ((endM - startM) + 77) /78;
for (Map.Entry<String, HBaseTableScanner.KeyRange> entry : scanPlan.entrySet()) {
    if(level == 1) {
        int jcount = 0;
        int sum1 = 0;
        int sum2 = 0;

        for(long j = thinTime; j < endThinTime; j += 60000) {
            jcount++;
            List<Integer> vehicleCountByDirection = getVehicleCountByDirection(entry.getKey(), j, j + 60000, startM, endM);

            // 如果是第一个表，初始化数据结构
            if(staList1.size() < jcount) {
                staList1.add(new SectionalFlowPiece(jcount, 0, 0, 0));
                staList2.add(new SectionalFlowPiece(jcount, 0, 0, 0));
            }

            // 累加到现有数据结构
            SectionalFlowPiece upPiece = staList1.get(jcount-1);
            SectionalFlowPiece downPiece = staList2.get(jcount-1);

            upPiece.setTotal((int) ((upPiece.getTotal() + vehicleCountByDirection.get(0))/factor));
            upPiece.setMinibus((int) ((upPiece.getMinibus() + vehicleCountByDirection.get(1))/factor));
            upPiece.setTruck(upPiece.getTotal()-upPiece.getMinibus());

            downPiece.setTotal((int) (downPiece.getTotal() + vehicleCountByDirection.get(3)/factor));
            downPiece.setMinibus((int) (downPiece.getMinibus() + vehicleCountByDirection.get(4)/factor));
            downPiece.setTruck(downPiece.getTotal()-downPiece.getMinibus());

            sum1 += upPiece.getTotal();
            sum2 += downPiece.getTotal();
        }

        totalSum1 += sum1;
        totalSum2 += sum2;
    }
    else if(level == 2) {
        Pair<List<List<Integer>>, List<String>> trafficStatsByStake = queryTrafficStats("traffic_stats_by_section", startTime, endTime, startM, endM);
        int i = 0;
        int sum1 = 0;
        int sum2 = 0;
        
        for(List<Integer> trafficStats : trafficStatsByStake.getKey()) {
            i++;
            
            // 如果是第一个表，初始化数据结构
            if(staList1.size() < i) {
                staList1.add(new SectionalFlowPiece(i, 0, 0, 0));
                staList2.add(new SectionalFlowPiece(i, 0, 0, 0));
            }
            
            // 累加到现有数据结构
            SectionalFlowPiece upPiece = staList1.get(i-1);
            SectionalFlowPiece downPiece = staList2.get(i-1);
            
            upPiece.setTotal(upPiece.getTotal() + trafficStats.get(0));
            upPiece.setMinibus(upPiece.getMinibus() + trafficStats.get(1));
            upPiece.setTruck(upPiece.getTruck() + trafficStats.get(2));
            
            downPiece.setTotal(downPiece.getTotal() + trafficStats.get(3));
            downPiece.setMinibus(downPiece.getMinibus() + trafficStats.get(4));
            downPiece.setTruck(downPiece.getTruck() + trafficStats.get(5));
            
            sum1 += trafficStats.get(0);
            sum2 += trafficStats.get(3);
        }
        
        totalSum1 += sum1;
        totalSum2 += sum2;
    }
    else if(level == 3) {
        Pair<List<List<Integer>>, List<String>> trafficStatsByStake = queryTrafficStats("traffic_stats_by_section", startTime, endTime, startM, endM);
        List<List<Integer>> lists = aggregateDailyStats(trafficStatsByStake.getKey(), trafficStatsByStake.getValue());
        int i = 0;
        int sum1 = 0;
        int sum2 = 0;
        
        for(List<Integer> trafficStats : lists) {
            i++;
            
            // 如果是第一个表，初始化数据结构
            if(staList1.size() < i) {
                staList1.add(new SectionalFlowPiece(i, 0, 0, 0));
                staList2.add(new SectionalFlowPiece(i, 0, 0, 0));
            }
            
            // 累加到现有数据结构
            SectionalFlowPiece upPiece = staList1.get(i-1);
            SectionalFlowPiece downPiece = staList2.get(i-1);
            
            upPiece.setTotal(upPiece.getTotal() + trafficStats.get(0));
            upPiece.setMinibus(upPiece.getMinibus() + trafficStats.get(1));
            upPiece.setTruck(upPiece.getTruck() + trafficStats.get(2));
            
            downPiece.setTotal(downPiece.getTotal() + trafficStats.get(3));
            downPiece.setMinibus(downPiece.getMinibus() + trafficStats.get(4));
            downPiece.setTruck(downPiece.getTruck() + trafficStats.get(5));
            
            sum1 += trafficStats.get(0);
            sum2 += trafficStats.get(3);
        }
        
        totalSum1 += sum1;
        totalSum2 += sum2;
    }
    else {
        List<Integer> se = se(entry.getKey(), startTime, endTime, startM, endM);
        totalSum1 += se.get(0);
        totalSum2 += se.get(1);
    }
}

// 在循环结束后构建最终结果
if(level == 1 || level == 2 || level == 3) {
    lsec.add(new SectionalFlowDat(1, totalSum1, staList1));
    lsec.add(new SectionalFlowDat(2, totalSum2, staList2));
    return new SectionalFlowData(lsec, inputParams);
}
else {
    lsec.add(new SectionalFlowDat(1, totalSum1, null));
    lsec.add(new SectionalFlowDat(2, totalSum2, null));
    return new SectionalFlowData(lsec, inputParams);
}


    }


   @Override
    public SectionalFlowResult getSectionalFlow(Long startTime, Long endTime,
                                               String startMileage, String endMileage,
                                               Double longitude1, Double latitude1,
                                               Double longitude2, Double latitude2,
                                               Integer level) throws IOException {

        return new SectionalFlowResult(200,"查询成功",getSingleSectionalFlow(startTime, endTime, startMileage, endMileage,
                                    longitude1, latitude1, longitude2, latitude2, level),true);
    }
    @Override
    public jizhanResult getStFlow(String stId, Long startTime, Long endTime) throws IOException {
        long time=System.currentTimeMillis();
        long st=startTime/3600000*3600000;
        long tt=endTime/3600000*3600000+3600000;
        List<String>l=new ArrayList<>();
        for(long i=st; i<tt; i+=3600000) {
            l.add(stId+"_"+i);
        }
        int[] data=getOne(l);

        jizhanUpDownCountData jizhanUpDownCountData=new jizhanUpDownCountData(data[0],data[1]);
        long time1=System.currentTimeMillis();

        return new jizhanResult(200,"查询成功",jizhanUpDownCountData,true,time1-time);
    }

    //http://100.65.38.139:8080/getByLongLati?startTime=1743158735648&endTime=1743158735690&Longitude1=114.04516&Latitude1=30.916416&Longitude2=114.045304&Latitude2=30.916420
    @Override
    public String getByLongLati(Long startTime, Long endTime, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {
        return null;
    }

    //http://100.65.38.139:8080/getBySkateID?startTime=1743158735648&endTime=1743158735690&startMileage=K1121&endMileage=K1125
    @Override
    public String getBySkateID(Long startTime, Long endTime, String startMileage, String endMileage) throws IOException {
        return "213";
    }
    public static List<VehicleSeg> direct(String jsonData) {
        List<VehicleSeg> l = new ArrayList<>();
     if (jsonData != null && !jsonData.isEmpty()) {
             if (jsonData.startsWith("\"") && jsonData.endsWith("\"")) {
                jsonData = jsonData.substring(1, jsonData.length() - 1).replace("\\\"", "\"");
            }

            try {
                JSONArray objects = JSON.parseArray(jsonData);
                for (Object object : objects) {
                    l.add(JSON.parseObject(object.toString(), VehicleSeg.class));
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return l;
    }
    public static List<VehicleSeg> ge(RedisTemplate<String, String> redisTemp, String redisKey) {
        String o = "";
        List<VehicleSeg> l = new ArrayList<>();
        String jsonData = redisTemp.opsForValue().get(redisKey);
//        System.out.println("redisKey"+redisKey+"jsonData:  " + jsonData);
//           Set<String> keys = redisTemp.keys("v*");
//        System.out.println("keys: "+keys);
        if (jsonData != null && !jsonData.isEmpty()) {
             if (jsonData.startsWith("\"") && jsonData.endsWith("\"")) {
                jsonData = jsonData.substring(1, jsonData.length() - 1).replace("\\\"", "\"");
            }

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
        }
        return l;
    }
}
