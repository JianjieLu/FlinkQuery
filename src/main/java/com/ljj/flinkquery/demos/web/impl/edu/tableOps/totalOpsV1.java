package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.TimeZone;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class totalOpsV1 {
    private static final String TABLE_NAME = "tabl_lane";
    private static final String COLUMN_FAMILY = "f1";
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String REDIS_KEY_PREFIX = "traffic_stats:";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static Configuration conf = HBaseConfiguration.create();
    private static JedisPool jedisPool;

    static {
        // 初始化配置
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 初始化Redis连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(32);
        poolConfig.setMinIdle(10);
        poolConfig.setMaxWaitMillis(100 * 1000);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 60000, REDIS_PASSWORD);

        // 设置日期格式的时区为UTC，假设数据存储使用UTC时间
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    // 添加方法：将时间字符串转换为时间戳
    public static long parseTimeString(String timeString) {
        try {
            return DATE_FORMAT.parse(timeString).getTime();
        } catch (ParseException e) {
            System.err.println("时间格式解析错误: " + timeString);
            throw new RuntimeException("时间格式解析错误", e);
        }
    }

    public static List<Map<String, Object>> queryTrafficData(String orgcode, long startTime, long endTime) {

        System.out.println("原始查询范围: startTime=" + startTime + " (" + new Date(startTime) +
                          "), endTime=" + endTime + " (" + new Date(endTime) + ")");

        // 计算小时窗口
        long startHour = (startTime / 3_600_000) * 3_600_000;
        long endHour = (endTime / 3_600_000) * 3_600_000 + 3_600_000;

        System.out.println("小时窗口: startHour=" + startHour + " (" + new Date(startHour) +
                          "), endHour=" + endHour + " (" + new Date(endHour) + ")");

        // 查询原始数据
        Map<Integer, Map<String, Double>> laneStats = queryRawTrafficData(orgcode, startHour, endHour);
//
//        System.out.println("查询结果车道数: " + laneStats.size());
//        for (Map.Entry<Integer, Map<String, Double>> entry : laneStats.entrySet()) {
//            System.out.println("车道 " + entry.getKey() + ": " + entry.getValue());
//        }

        // 按方向组织结果
        return formatTrafficDataByDirection(laneStats);
    }

    public static Map<Integer, Map<String, Double>> queryRawTrafficData(String orgcode, long startHour, long endHour) {
        Map<Integer, Map<String, Double>> laneStats = new HashMap<>();

        System.out.println("开始查询Redis和HBase数据，时间范围: " + startHour + " 到 " + endHour);

        // 并行查询 Redis 和 HBase
        Map<Integer, Map<String, Double>> redisResult = queryFromRedis(orgcode, startHour, endHour);
        Map<Integer, Map<String, Double>> hbaseResult = queryFromHBase(orgcode, startHour, endHour);

        System.out.println("Redis查询结果: " + redisResult.size() + " 个车道");
        System.out.println("HBase查询结果: " + hbaseResult.size() + " 个车道");

        // 合并结果
        mergeResults(laneStats, redisResult);
        mergeResults(laneStats, hbaseResult);

        System.out.println("合并后结果: " + laneStats.size() + " 个车道");

        // 计算平均速度
        calculateAverageSpeed(laneStats);

        return laneStats;
    }

    private static Map<Integer, Map<String, Double>> queryFromRedis(String orgcode, long startHour, long endHour) {
        Map<Integer, Map<String, Double>> result = new HashMap<>();
        Map<Integer, Double> totalSpeeds = new HashMap<>();
        Map<Integer, Integer> vehicleCounts = new HashMap<>();

        int keyCount = 0;
        int laneCount = 0;

        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("开始Redis查询，键前缀: " + REDIS_KEY_PREFIX + orgcode + "_");

            // 直接构造所有可能的键，避免全量扫描
            for (long hour = startHour; hour < endHour; hour += 3600000) {
                String key = REDIS_KEY_PREFIX + orgcode + "_" + hour;

                if (!jedis.exists(key)) {
                    continue;
                }

                keyCount++;
                Map<String, String> allLanes = jedis.hgetAll(key);

                for (Map.Entry<String, String> entry : allLanes.entrySet()) {
                    laneCount++;
                    String laneKey = entry.getKey();

                    // 解析车道号
                    int lane;
                    try {
                        lane = Integer.parseInt(laneKey.substring(5)); // "lane_14" -> 14
                    } catch (NumberFormatException e) {
                        System.err.println("无效的车道键格式: " + laneKey);
                        continue;
                    }

                    String statsJson = entry.getValue();
                    JSONObject statsMap;

                    try {
                        statsMap = JSON.parseObject(statsJson);
                    } catch (Exception e) {
                        System.err.println("JSON解析失败: " + statsJson);
                        continue;
                    }

                    // 初始化车道统计
                    Map<String, Double> laneStats = result.computeIfAbsent(lane, k -> new HashMap<>());

                    // 累加计数
                    laneStats.merge("busCount", (double)statsMap.getIntValue("busCount"), Double::sum);
                    laneStats.merge("trackCount", (double)statsMap.getIntValue("trackCount"), Double::sum);
                    laneStats.merge("totalCount", (double)statsMap.getIntValue("totalCount"), Double::sum);

                    // 累加速度相关数据
                    double totalSpeed = statsMap.getDoubleValue("totalSpeed");
                    int vehicleCount = statsMap.getIntValue("vehicleCount");

                    totalSpeeds.merge(lane, totalSpeed, Double::sum);
                    vehicleCounts.merge(lane, vehicleCount, Integer::sum);
                }
            }

            System.out.println("Redis查询完成，找到 " + keyCount + " 个键，" + laneCount + " 个车道数据");
        } catch (Exception e) {
            System.err.println("Redis 查询失败: " + e.getMessage());
            e.printStackTrace();
        }

        // 将总速度和车辆数存入结果
        for (Integer lane : result.keySet()) {
            result.get(lane).put("totalSpeed", totalSpeeds.getOrDefault(lane, 0.0));
            result.get(lane).put("vehicleCount", (double)vehicleCounts.getOrDefault(lane, 0));
        }

        return result;
    }

    private static Map<Integer, Map<String, Double>> queryFromHBase(String orgcode, long startHour, long endHour) {
        Map<Integer, Map<String, Double>> result = new HashMap<>();
        int rowCount = 0;

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            // 构建扫描器
            Scan scan = new Scan();

            // 设置 RowKey 范围
            String startRow = orgcode + "_" + startHour + "_";
            String stopRow = orgcode + "_" + endHour + "_";

            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));

            // 添加需要查询的列
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("busCount"));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("trackCount"));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalCount"));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalSpeed"));
            scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("vehicleCount"));

            // 设置扫描性能参数
            scan.setCaching(1000); // 每次RPC返回的行数
            scan.setBatch(100); // 每次返回的列数

            System.out.println("HBase扫描范围: startRow=" + startRow + ", stopRow=" + stopRow);

            // 执行扫描
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result res : scanner) {
                    rowCount++;

                    // 解析 RowKey 获取车道号
                    String rowKey = Bytes.toString(res.getRow());
                    String[] parts = rowKey.split("_");

                    if (parts.length < 3) {
                        System.err.println("无效的RowKey格式: " + rowKey);
                        continue;
                    }

                    int lane;
                    try {
                        lane = Integer.parseInt(parts[2]);
                    } catch (NumberFormatException e) {
                        System.err.println("无效的车道号格式: " + parts[2]);
                        continue;
                    }

                    // 获取统计数据
                    Map<String, Double> stats = result.computeIfAbsent(lane, k -> new HashMap<>());

                    // 辅助函数：获取列值并转换为double
                    Function<byte[], Double> getDoubleValue = (bytes) -> {
                        if (bytes == null) return 0.0;
                        try {
                            return Double.parseDouble(Bytes.toString(bytes));
                        } catch (NumberFormatException e) {
                            System.err.println("数值格式错误: " + Bytes.toString(bytes));
                            return 0.0;
                        }
                    };

                    // 获取各列的值
                    double busCount = getDoubleValue.apply(res.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("busCount")));
                    double trackCount = getDoubleValue.apply(res.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("trackCount")));
                    double totalCount = getDoubleValue.apply(res.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalCount")));
                    double totalSpeed = getDoubleValue.apply(res.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalSpeed")));
                    double vehicleCount = getDoubleValue.apply(res.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("vehicleCount")));

                    // 累加统计值
                    stats.merge("busCount", busCount, Double::sum);
                    stats.merge("trackCount", trackCount, Double::sum);
                    stats.merge("totalCount", totalCount, Double::sum);
                    stats.merge("totalSpeed", totalSpeed, Double::sum);
                    stats.merge("vehicleCount", vehicleCount, Double::sum);
                }
            }

            System.out.println("HBase扫描完成，找到 " + rowCount + " 行数据");
        } catch (IOException e) {
            System.err.println("HBase 查询失败: " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("数据格式错误: " + e.getMessage());
            e.printStackTrace();
        }

        return result;
    }

    private static void mergeResults(Map<Integer, Map<String, Double>> target,
                                     Map<Integer, Map<String, Double>> source) {

        for (Map.Entry<Integer, Map<String, Double>> entry : source.entrySet()) {
            int lane = entry.getKey();
            Map<String, Double> sourceStats = entry.getValue();
            Map<String, Double> targetStats = target.computeIfAbsent(lane, k -> new HashMap<>());

            // 合并统计值
            for (Map.Entry<String, Double> stat : sourceStats.entrySet()) {
                targetStats.merge(stat.getKey(), stat.getValue(), Double::sum);
            }
        }
    }

    private static void calculateAverageSpeed(Map<Integer, Map<String, Double>> laneStats) {
        for (Map<String, Double> stats : laneStats.values()) {
            double totalSpeed = stats.getOrDefault("totalSpeed", 0.0);
            double vehicleCount = stats.getOrDefault("vehicleCount", 0.0);
            // 计算平均速度
            double aveSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;
            // 使用BigDecimal进行四舍五入
            BigDecimal bd = new BigDecimal(aveSpeed);
            bd = bd.setScale(2, RoundingMode.HALF_UP); // 参数2表示保留两位，HALF_UP表示四舍五入
            double roundedAveSpeed = bd.doubleValue();
            stats.put("aveSpeed", roundedAveSpeed);
        }
    }

    private static List<Map<String, Object>> formatTrafficDataByDirection(Map<Integer, Map<String, Double>> laneStats) {
        List<Map<String, Object>> directions = new ArrayList<>();

        // 创建方向1（上行）和方向2（下行）的数据结构
        Map<String, Object> direction1 = new LinkedHashMap<>();
        direction1.put("direction", 1);
        direction1.put("total", 0);
        direction1.put("minibus", 0);
        direction1.put("truck", 0);
        direction1.put("aveSpeed", 0.0);
        List<Map<String, Object>> laneList1 = new ArrayList<>();
        direction1.put("laneList", laneList1);

        Map<String, Object> direction2 = new LinkedHashMap<>();
        direction2.put("direction", 2);
        direction2.put("total", 0);
        direction2.put("minibus", 0);
        direction2.put("truck", 0);
        direction2.put("aveSpeed", 0.0);
        List<Map<String, Object>> laneList2 = new ArrayList<>();
        direction2.put("laneList", laneList2);

        // 用于计算方向平均速度的变量
        Map<Integer, Double> directionTotalSpeeds = new HashMap<>();
        Map<Integer, Integer> directionVehicleCounts = new HashMap<>();

        // 初始化
        directionTotalSpeeds.put(1, 0.0);
        directionTotalSpeeds.put(2, 0.0);
        directionVehicleCounts.put(1, 0);
        directionVehicleCounts.put(2, 0);

        // 遍历所有车道数据
        for (Map.Entry<Integer, Map<String, Double>> entry : laneStats.entrySet()) {
            int lane = entry.getKey();
            Map<String, Double> stats = entry.getValue();

            int busCount = stats.getOrDefault("busCount", 0.0).intValue();
            int trackCount = stats.getOrDefault("trackCount", 0.0).intValue();
            int totalCount = stats.getOrDefault("totalCount", 0.0).intValue();
            double aveSpeed = stats.getOrDefault("aveSpeed", 0.0);
            double totalSpeed = stats.getOrDefault("totalSpeed", 0.0);
            int vehicleCount = stats.getOrDefault("vehicleCount", 0.0).intValue();

            // 创建车道数据 Map
            Map<String, Object> laneData = new LinkedHashMap<>();
            laneData.put("lane", lane);
            laneData.put("total", totalCount);
            laneData.put("minibus", busCount);
            laneData.put("truck", trackCount);
            laneData.put("aveSpeed", aveSpeed);

            // 根据车道号判断方向（奇数车道为上行，偶数车道为下行）
            int direction = (lane % 2 == 1) ? 1 : 2;
            Map<String, Object> directionObj;
            List<Map<String, Object>> laneList;

            if (direction == 1) {
                directionObj = direction1;
                laneList = laneList1;
            } else {
                directionObj = direction2;
                laneList = laneList2;
            }

            // 累加方向统计数据
            directionObj.put("total", (int)directionObj.get("total") + totalCount);
            directionObj.put("minibus", (int)directionObj.get("minibus") + busCount);
            directionObj.put("truck", (int)directionObj.get("truck") + trackCount);

            // 累加速度相关数据
            directionTotalSpeeds.merge(direction, totalSpeed, Double::sum);
            directionVehicleCounts.merge(direction, vehicleCount, Integer::sum);

            // 添加车道数据到方向列表
            laneList.add(laneData);
        }

        // 计算方向平均速度
        for (int dir : new int[]{1, 2}) {
            double totalSpeed = directionTotalSpeeds.get(dir);
            int vehicleCount = directionVehicleCounts.get(dir);
            double aveSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;

            if (dir == 1) {
                direction1.put("aveSpeed", Math.round((aveSpeed * 100.0)) / 100.0);
            } else {
                direction2.put("aveSpeed", Math.round((aveSpeed * 100.0)) / 100.0);
            }
        }

        // 创建结果列表
        directions.add(direction1);
        directions.add(direction2);

        return directions;
    }
}