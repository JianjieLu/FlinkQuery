package com.ljj.flinkquery.demos.web.service;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.web.impl.edu.hbaseTool;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;
import com.ljj.flinkquery.demos.entity.TrafficEventUtils.*;

import com.ljj.flinkquery.demos.web.impl.edu.tableOps.*;
import com.ljj.flinkquery.demos.entity.stakeEnvents.*;
import com.ljj.flinkquery.demos.entity.GeoUtils.*;


import static com.ljj.flinkquery.FlinkQueryApplication.resultMap;
import static com.ljj.flinkquery.demos.entity.data.Utils.convertFromTimestampMillis;
//import static com.ljj.flinkquery.FlinkQueryApplication.redisTemplate;
@Service
public class HBaseServiceImpl implements HBaseService {
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

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
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141");  // Zookeeper 地址
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

    @Override
    public TimeSpatialResult getByTimeSpatial(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException {
//        Set<String> keys = redisTemplate.keys("v*");
//        System.out.println(keys);
        long t1 = System.currentTimeMillis();
            //region Description

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
            long st = startTime / 10000 * 10000;
            long tt = endTime / 10000 * 10000 + 10000;
            String zaStartMil = "";
            String zaEndMil = "";
            TimeSpatialData kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            TimeSpatialResult t2 = new TimeSpatialResult(200, "内存查找————无范围内数据,原因：桩号转换失败", kong, true);
            TimeSpatialData tos = new TimeSpatialData();
            boolean za = true;
            boolean main = true;
            StakeAssignment stakeAssign;
            int index;
            int index1;String startMil;String endMil;
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
Set<String> keys = redisTemplate.keys("v*");
            System.out.println("keys: "+keys);
        System.out.println(System.currentTimeMillis());
        boolean b = Objects.equals(startMileage, "") && Objects.equals(endMileage, "");
        if(startTime>System.currentTimeMillis()-120000&&endTime<System.currentTimeMillis()-9000) {
            System.out.println("开始内存查找：http://100.65.38.139:8080/getByTimeSpatial?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);

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
                System.out.println("AK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);//zaStartMil:  0  zaEndMil:  0
                System.out.println("AK  startSK:  " + startSK + "  endSK:  " + endSK);// startSK:  BK0+373.5  endSK:  BK0+688
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;

                zadaoInfo = zadaoInfo + "AK" + startM + "to AK" + endM + "," + st + " to " + tt + "  ";
                for (long i = st; i < tt; i += 10000) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v" + i + "_AK" + j;
                        System.out.println(redisKey);
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
                System.out.println("BK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("BK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "BK" + startM + "to BK" + endM + "," + st + " to " + tt + "  ";

                for (long i = st; i < tt; i += 10000) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v" + i + "_BK" + j;
                        System.out.println(redisKey);
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
                System.out.println("CK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("CK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "CK" + startM + "to CK" + endM + "," + st + " to " + tt + "  ";
                for (long i = st; i < tt; i += 10000) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v" + i + "_CK" + j;
                        System.out.println(redisKey);
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
                System.out.println("DK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("DK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "DK" + startM + "to DK" + endM + "," + st + " to " + tt + "  ";
                for (long i = st; i < tt; i += 10000) {
                    for (int j = startM; j < endM; j++) {
                        String redisKey = "v" + i + "_DK" + j;
                        System.out.println(redisKey);
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
                    if(vt != null) {
                         if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                    else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                        zuphuoche++;
                    else if (vt == 8) {
                        zupweihuaping++;
                        zuphuoche++;
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
                tos.setZaCongestionIndex(Math.round((sum / n / 120) * 1000.0) / 1000.0);
                tos.setZaBusCount(zupkeche);
                tos.setZaTrackCount(zuphuoche);
                tos.setZaChemicalCount(zupweihuaping);
                tos.setZaHeavyTrackCount(zupzhongxinghuoche);
                tos.setBusTrackVal(busTrackVal);
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
                for (long i = st; i < tt; i += 10000) {
                    for (int j = startM; j < endM; j++) {

                        String redisKey = "v" + i + "_K" + j;

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
                    if(vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            uphuoche++;
                        else if (vt == 8) {
                            upweihuaping++;
                            uphuoche++;
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
                    if(vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            downhuoche++;
                        else if (vt == 8) {
                            downweihuaping++;
                            downhuoche++;
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
//            System.out.println("timestamp: from " + st + "(" + startTime + ") to " + tt + "(" + endTime + ")  SkateID: from " + startMi + "(" + startMileage + ") to " + endMi + "(" + endMileage + ")");
            if (n != 0) {
                double chemidu = Math.round(((double) m.size() / (endM - startM) * 100.0)) / 100.0;
                double busTrackVal = Math.round((double) (downkeche + upkeche) / (uphuoche + downhuoche) * 100.0) / 100.0;
                double upbusTrackVal = Math.round((double) (upkeche) / (uphuoche) * 100.0) / 100.0;
                double downbusTrackVal = Math.round((double) (downkeche) / (downhuoche) * 100.0) / 100.0;
                tos.setTotalAverageSpeed(Math.round((sum / n * 100.0)) / 100.0);
                tos.setUpAverageSpeed(Math.round((shangxingSum / shangxing1 * 100.0)) / 100.0);
                tos.setDownAverageSpeed(Math.round(xiaxingSum / xiaxing2 * 100.0) / 100.0);
                tos.setTotalCount((int) n);
                tos.setUpCount(shangxing1);
                tos.setDownCount(xiaxing2);
                tos.setTrafficSaturation(Math.round(n / ((double) (endM - startM)) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setVehicleDensity(chemidu);
                tos.setTotalCongestionIndex(Math.round((sum / n / 120) * 1000.0) / 1000.0);
                tos.setUpCongestionIndex(Math.round((shangxingSum / shangxing1 / 120) * 1000.0) / 1000.0);
                tos.setDownCongestionIndex(Math.round((xiaxingSum / xiaxing2 / 120) * 1000.0) / 1000.0);
                tos.setUpBusCount(upkeche);
                tos.setUpTrackCount(uphuoche);
                tos.setUpChemicalCount(upweihuaping);
                tos.setUpHeavyTrackCount(upzhongxinghuoche);
                tos.setDownBusCount(downkeche);
                tos.setDownTrackCount(downhuoche);
                tos.setDownChemicalCount(downweihuaping);
                tos.setDownHeavyTrackCount(downzhongxinghuoche);
                tos.setBusTrackVal(busTrackVal);
                tos.setUpBusTrackVal(upbusTrackVal);
                tos.setDownBusTrackVal(downbusTrackVal);
            } else main = false;

            //endregion
            //region Description
            if (main && za) {

                return new TimeSpatialResult(200, "内存查找————匝道、主路均有数据    查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo+  "   查询主路路段数："+(endM-startM)+"   查询时段数："+(tt-st)/10000, tos, true);
            }
            else if (main && !za) return new TimeSpatialResult(200, "内存查找————主路有数据,匝道无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM +"   查询主路路段数："+(endM-startM)+"   查询时段数："+(tt-st)/10000, tos, true);
            else if (!main && za) return new TimeSpatialResult(200, "内存查找————主路无数据,匝道有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo+"   查询时段数："+(tt-st)/10000, tos, true);
            else return new TimeSpatialResult(200, "内存查找————主路、匝道均无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);

        }else if(startTime>System.currentTimeMillis()-9000 || endTime>System.currentTimeMillis()-9000) {
            return new TimeSpatialResult(200, "时间超过当前时间或内存统计时间" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) , kong, true);
        }
        else {
            System.out.println("开始数据库查找，查找语句：http://100.65.38.139:8080/getByTimeSpatialWithID?startTime=" + startTime + "&endTime=" + endTime + "&startMileage=" + startMileage + "&endMileage=" + endMileage + "&Longitude1=" + Longitude1 + "&Latitude1=" + Latitude1 + "&Longitude2=" + Longitude2 + "&Latitude2=" + Latitude1);
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
            kong = new TimeSpatialData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            t2 = new TimeSpatialResult(200, "数据库查找————无范围内数据,原因：桩号转换失败", kong, true);
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

            //AK
            if (MBR.hasIntersection(new MBR(114.03852081298828, 114.04580688476562, 30.91611099243164, 30.919893264770508), new MBR(Longitude1, Longitude2, Latitude1, Latitude2))) {
                String startSK = LocationOP.GETLonNearest(Longitude1, roadAKDataList).getLocation();
                String endSK = LocationOP.GETLonNearest(Longitude2, roadAKDataList).getLocation();
                index = startSK.indexOf("+");
                zaStartMil = ((index != -1) ? startSK.substring(2, index) : startSK);
                index1 = endSK.indexOf("+");
                zaEndMil = ((index1 != -1) ? endSK.substring(2, index1) : endSK);
                System.out.println("AK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("AK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "AK" + startM + "to AK" + endM + "," + st + " to " + tt + "  ";
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
                System.out.println("BK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("BK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "BK" + startM + "to BK" + endM + "," + st + " to " + tt + "  ";
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
                System.out.println("CK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("CK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "CK" + startM + "to CK" + endM + "," + st + " to " + tt + "  ";
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
                System.out.println("DK  zaStartMil:  " + zaStartMil + "  zaEndMil:  " + zaEndMil);
                System.out.println("DK  startSK:  " + startSK + "  endSK:  " + endSK);
                //从起始时间到终止时间
                startM = Integer.parseInt(zaStartMil);
                endM = Integer.parseInt(zaEndMil);
                if (startM > endM) {
                    int temp = endM;
                    endM = startM;
                    startM = temp;
                }
                endM+=1;
                zdeltam += endM - startM;
                zadaoInfo = zadaoInfo + "DK" + startM + "to DK" + endM + "," + st + " to " + tt + "  ";
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
                    if(vt != null) {
                    if (vt == 1 || vt == 3 || vt == 7 || vt == 15) zupkeche++;
                    else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                        zuphuoche++;
                    else if (vt == 8) {
                        zupweihuaping++;
                        zuphuoche++;
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
                tos.setZaCongestionIndex(Math.round((sum / n / 120) * 1000.0) / 1000.0);
                tos.setZaBusCount(zupkeche);
                tos.setZaTrackCount(zuphuoche);
                tos.setZaChemicalCount(zupweihuaping);
                tos.setZaHeavyTrackCount(zupzhongxinghuoche);
                tos.setBusTrackVal(busTrackVal);
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
                for (long i = st; i < tt; i += 60000) {
                    for (int j = startM; j < endM; j++) {
                        List<VehicleSeg> l = totalOps.getVeByRowkey(hbaseTool.convertToHBaseTableName(i), i + "_K" + j);
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
                    if(vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) upkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            uphuoche++;
                        else if (vt == 8) {
                            upweihuaping++;
                            uphuoche++;
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
                    if(vt != null) {
                        if (vt == 1 || vt == 3 || vt == 7 || vt == 15) downkeche++;
                        else if (vt == 2 || vt == 10 || vt == 11 || vt == 170 || vt == 171 || vt == 172 || vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177)
                            downhuoche++;
                        else if (vt == 8) {
                            downweihuaping++;
                            downhuoche++;
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
            System.out.println("timestamp: from " + st + "(" + startTime + ") to " + tt + "(" + endTime + ")  SkateID: from " + startMi + "(" + startMileage + ") to " + endMi + "(" + endMileage + ")");
            if (n != 0) {
                double chemidu = Math.round(((double) m.size() / (endM - startM) * 100.0)) / 100.0;
                double busTrackVal = Math.round((double) (downkeche + upkeche) / (uphuoche + downhuoche) * 100.0) / 100.0;
                double upbusTrackVal = Math.round((double) (upkeche) / (uphuoche) * 100.0) / 100.0;
                double downbusTrackVal = Math.round((double) (downkeche) / (downhuoche) * 100.0) / 100.0;
                tos.setTotalAverageSpeed(Math.round((sum / n * 100.0)) / 100.0);
                tos.setUpAverageSpeed(Math.round((shangxingSum / shangxing1 * 100.0)) / 100.0);
                tos.setDownAverageSpeed(Math.round(xiaxingSum / xiaxing2 * 100.0) / 100.0);
                tos.setTotalCount((int) n);
                tos.setUpCount(shangxing1);
                tos.setDownCount(xiaxing2);
                tos.setTrafficSaturation(Math.round(n / ((double) (endM - startM)) / ((double) (tt - st) / 60000) / ((double) 2200 / 60) * 100.0) / 100.0);
                tos.setVehicleDensity(chemidu);
                tos.setTotalCongestionIndex(Math.round((sum / n / 120) * 1000.0) / 1000.0);
                tos.setUpCongestionIndex(Math.round((shangxingSum / shangxing1 / 120) * 1000.0) / 1000.0);
                tos.setDownCongestionIndex(Math.round((xiaxingSum / xiaxing2 / 120) * 1000.0) / 1000.0);
                tos.setUpBusCount(upkeche);
                tos.setUpTrackCount(uphuoche);
                tos.setUpChemicalCount(upweihuaping);
                tos.setUpHeavyTrackCount(upzhongxinghuoche);
                tos.setDownBusCount(downkeche);
                tos.setDownTrackCount(downhuoche);
                tos.setDownChemicalCount(downweihuaping);
                tos.setDownHeavyTrackCount(downzhongxinghuoche);
                tos.setBusTrackVal(busTrackVal);
                tos.setUpBusTrackVal(upbusTrackVal);
                tos.setDownBusTrackVal(downbusTrackVal);
            } else {
                main = false;
            }
            if (main && za) {
                return new TimeSpatialResult(200, "数据库查找————匝道、主路均有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo+"   查询主路路段数："+(endM-startM)+"   查询时段数："+(tt-st)/60000, tos, true);
            } else if (main && !za)
                return new TimeSpatialResult(200, "数据库查找————主路有数据,匝道无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM +"   查询主路路段数："+(endM-startM)+"   查询时段数："+(tt-st)/60000, tos, true);
            else if (!main && za) return new TimeSpatialResult(200, "数据库查找————主路无数据,匝道有数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
            else return new TimeSpatialResult(200, "数据库查找————主路、匝道均无数据,查询用时：" + (System.currentTimeMillis() - t1) + "ms   查询时间段：" + convertFromTimestampMillis(st) + " to " + convertFromTimestampMillis(tt) + ",桩号：K" + startM + " to K" + endM + "  " + zadaoInfo, kong, true);
        }
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
