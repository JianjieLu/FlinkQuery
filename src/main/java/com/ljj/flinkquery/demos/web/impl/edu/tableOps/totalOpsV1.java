package com.ljj.flinkquery.demos.web.impl.edu.tableOps;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

public class totalOpsV1 {
    private static final String TABLE_NAME = "tabl_lane";
    private static final String COLUMN_FAMILY = "f1";
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String REDIS_KEY_PREFIX = "traffic_stats:";

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
    }

    public static List<Map<String, Object>> queryTrafficData(String orgcode, long startTime, long endTime) {
    // 计算小时窗口
    long startHour = (startTime / 3_600_000) * 3_600_000;
    long endHour = (endTime / 3_600_000) * 3_600_000 + 3_600_000;

    // 查询原始数据
    Map<Integer, Map<String, Double>> laneStats = queryRawTrafficData(orgcode, startHour, endHour);

    // 按方向组织结果
    return formatTrafficDataByDirection(laneStats);
}

public static Map<Integer, Map<String, Double>> queryRawTrafficData(String orgcode, long startHour, long endHour) {
    Map<Integer, Map<String, Double>> laneStats = new HashMap<>();

    // 并行查询 Redis 和 HBase
    Map<Integer, Map<String, Double>> redisResult = queryFromRedis(orgcode, startHour, endHour);
    Map<Integer, Map<String, Double>> hbaseResult = queryFromHBase(orgcode, startHour, endHour);

    // 合并结果
    mergeResults(laneStats, redisResult);
    mergeResults(laneStats, hbaseResult);

    // 计算平均速度
    calculateAverageSpeed(laneStats);

    return laneStats;
}

private static Map<Integer, Map<String, Double>> queryFromRedis(String orgcode, long startHour, long endHour) {
    Map<Integer, Map<String, Double>> result = new HashMap<>();
    Map<Integer, Double> totalSpeeds = new HashMap<>();
    Map<Integer, Integer> vehicleCounts = new HashMap<>();

    try (Jedis jedis = jedisPool.getResource()) {
        // 直接构造所有可能的键，避免全量扫描
        for (long hour = startHour; hour <= endHour; hour += 3600000) {
            String key = REDIS_KEY_PREFIX + orgcode + "_" + hour;

            if (!jedis.exists(key)) continue;

            Map<String, String> allLanes = jedis.hgetAll(key);

            for (Map.Entry<String, String> entry : allLanes.entrySet()) {
                String laneKey = entry.getKey();
                int lane = Integer.parseInt(laneKey.substring(5)); // "lane_14" -> 14

                String statsJson = entry.getValue();
                JSONObject statsMap = JSON.parseObject(statsJson);

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
    } catch (Exception e) {
        System.err.println("Redis 查询失败: " + e.getMessage());
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

    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

        // 构建扫描器
        Scan scan = new Scan();

        // 设置 RowKey 范围
        scan.setStartRow(Bytes.toBytes(orgcode + "_" + startHour + "_"));
        scan.setStopRow(Bytes.toBytes(orgcode + "_" + endHour + "_"));

        // 添加需要查询的列
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("busCount"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("trackCount"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalCount"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("totalSpeed"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("vehicleCount"));

        // 执行扫描
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result res : scanner) {
                // 解析 RowKey 获取车道号
                String rowKey = Bytes.toString(res.getRow());
                String[] parts = rowKey.split("_");
                int lane = Integer.parseInt(parts[2]);

                // 获取统计数据
                Map<String, Double> stats = result.computeIfAbsent(lane, k -> new HashMap<>());

                // 辅助函数：获取列值并转换为double
                Function<byte[], Double> getDoubleValue = (bytes) -> {
                    if (bytes == null) return 0.0;
                    return Double.parseDouble(Bytes.toString(bytes));
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
    } catch (IOException e) {
        System.err.println("HBase 查询失败: " + e.getMessage());
    } catch (NumberFormatException e) {
        System.err.println("数据格式错误: " + e.getMessage());
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
        stats.put("aveSpeed", aveSpeed);
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

        int busCount = stats.get("busCount").intValue();
        int trackCount = stats.get("trackCount").intValue();
        int totalCount = stats.get("totalCount").intValue();
        double aveSpeed = stats.get("aveSpeed");
        double totalSpeed = stats.get("totalSpeed");
        int vehicleCount = stats.get("vehicleCount").intValue();

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
            direction1.put("aveSpeed", aveSpeed);
        } else {
            direction2.put("aveSpeed", aveSpeed);
        }
    }

    // 创建结果列表
    directions.add(direction1);
    directions.add(direction2);

    return directions;
}
}