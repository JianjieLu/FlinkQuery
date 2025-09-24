package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ljj.flinkquery.demos.entity.upDownResult;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.TimeZone;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class totalOpsV2 {
    private static final String TABLE_NAME = "tabl_lane";
    private static final String COLUMN_FAMILY = "f1";
    private static final String REDIS_HOST = "100.65.38.141";
    private static final int REDIS_PORT = 6380;
    private static final String REDIS_PASSWORD = "whdx123cgz666";
    private static final String REDIS_KEY_PREFIX = "traffic_stats:";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
    private static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("yyyy-MM-01 00:00:00");

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
        HOUR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        MONTH_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
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

    // 根据level生成时间区间
    private static List<TimeRange> generateTimeRanges(long startTime, long endTime, int level) {
        List<TimeRange> timeRanges = new ArrayList<>();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(startTime);

        switch (level) {
            case 1: // 按小时统计
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                while (calendar.getTimeInMillis() < endTime) {
                    long rangeStart = calendar.getTimeInMillis();
                    calendar.add(Calendar.HOUR, 1);
                    long rangeEnd = calendar.getTimeInMillis();

                    if (rangeStart < endTime) {
                        timeRanges.add(new TimeRange(rangeStart, Math.min(rangeEnd, endTime)));
                    }
                }
                break;

            case 2: // 按天统计
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                while (calendar.getTimeInMillis() < endTime) {
                    long rangeStart = calendar.getTimeInMillis();
                    calendar.add(Calendar.DAY_OF_MONTH, 1);
                    long rangeEnd = calendar.getTimeInMillis();

                    if (rangeStart < endTime) {
                        timeRanges.add(new TimeRange(rangeStart, Math.min(rangeEnd, endTime)));
                    }
                }
                break;

            case 3: // 按月统计
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);

                while (calendar.getTimeInMillis() < endTime) {
                    long rangeStart = calendar.getTimeInMillis();
                    calendar.add(Calendar.MONTH, 1);
                    long rangeEnd = calendar.getTimeInMillis();

                    if (rangeStart < endTime) {
                        timeRanges.add(new TimeRange(rangeStart, Math.min(rangeEnd, endTime)));
                    }
                }
                break;

            default:
                throw new IllegalArgumentException("不支持的统计级别: " + level);
        }

        return timeRanges;
    }

    // 时间区间类
    private static class TimeRange {
        long start;
        long end;
        String timeLabel;
        int timeIndex;

        TimeRange(long start, long end) {
            this.start = start;
            this.end = end;

            // 根据时间区间长度生成标签
            long duration = end - start;
            if (duration <= 3600000) { // 1小时
                this.timeLabel = HOUR_FORMAT.format(new Date(start));
            } else if (duration <= 86400000) { // 1天
                this.timeLabel = DAY_FORMAT.format(new Date(start));
            } else { // 1个月或更多
                this.timeLabel = MONTH_FORMAT.format(new Date(start));
            }
        }

        void setTimeIndex(int index) {
            this.timeIndex = index;
        }
    }

    public static Map<String, Object> queryTrafficData(String orgcode, long startTime, long endTime, int level) {
        System.out.println("原始查询范围: startTime=" + startTime + " (" + new Date(startTime) +
                          "), endTime=" + endTime + " (" + new Date(endTime) + "), level=" + level);

        // 生成时间区间
        List<TimeRange> timeRanges = generateTimeRanges(startTime, endTime, level);

        // 设置时间索引
        for (int i = 0; i < timeRanges.size(); i++) {
            timeRanges.get(i).setTimeIndex(i + 1);
        }

        // 存储每个方向的统计数据
        Map<Integer, List<Map<String, Object>>> directionStats = new HashMap<>();
        directionStats.put(1, new ArrayList<>()); // 方向1：上行
        directionStats.put(2, new ArrayList<>()); // 方向2：下行

        // 对每个时间区间进行查询
        for (TimeRange timeRange : timeRanges) {
            System.out.println("处理时间区间: " + timeRange.timeLabel + " [" +
                              new Date(timeRange.start) + " - " + new Date(timeRange.end) + "]");

            // 计算小时窗口
            long startHour = (timeRange.start / 3_600_000) * 3_600_000;
            long endHour = (timeRange.end / 3_600_000) * 3_600_000 + 3_600_000;

            // 查询原始数据
            Map<Integer, Map<String, Double>> laneStats = queryRawTrafficData(orgcode, startHour, endHour);

            System.out.println("查询结果车道数: " + laneStats.size());
            for (Map.Entry<Integer, Map<String, Double>> entry : laneStats.entrySet()) {
                System.out.println("车道 " + entry.getKey() + ": " + entry.getValue());
            }

            // 按方向汇总数据
            Map<Integer, Map<String, Object>> timeRangeStats = aggregateTrafficDataByDirection(laneStats);

            // 为每个方向添加时间区间统计
            for (int direction : new int[]{1, 2}) {
                if (timeRangeStats.containsKey(direction)) {
                    Map<String, Object> stats = timeRangeStats.get(direction);
                    stats.put("time", timeRange.timeIndex);
                    directionStats.get(direction).add(stats);
                } else {
                    // 如果没有数据，添加空统计
                    Map<String, Object> emptyStats = new HashMap<>();
                    emptyStats.put("time", timeRange.timeIndex);
                    emptyStats.put("total", 0);
                    emptyStats.put("minibus", 0);
                    emptyStats.put("truck", 0);
                    emptyStats.put("speed", 0.0);
                    directionStats.get(direction).add(emptyStats);
                }
            }
        }

        // 构建最终结果
        Map<String, Object> result = new HashMap<>();
        result.put("stationId", orgcode);

        List<Map<String, Object>> dirStaList = new ArrayList<>();

        // 方向1
        Map<String, Object> direction1 = new HashMap<>();
        direction1.put("direction", 1);
        direction1.put("staList", directionStats.get(1));
        dirStaList.add(direction1);

        // 方向2
        Map<String, Object> direction2 = new HashMap<>();
        direction2.put("direction", 2);
        direction2.put("staList", directionStats.get(2));
        dirStaList.add(direction2);

        result.put("dirStaList", dirStaList);

        return result;
    }

    // 按方向汇总交通数据（不按车道细分）
    private static Map<Integer, Map<String, Object>> aggregateTrafficDataByDirection(Map<Integer, Map<String, Double>> laneStats) {
        Map<Integer, Map<String, Object>> directionStats = new HashMap<>();

        // 初始化两个方向的统计
        Map<String, Object> dir1Stats = new HashMap<>();
        dir1Stats.put("total", 0);
        dir1Stats.put("minibus", 0);
        dir1Stats.put("truck", 0);
        dir1Stats.put("totalSpeed", 0.0);
        dir1Stats.put("vehicleCount", 0);

        Map<String, Object> dir2Stats = new HashMap<>();
        dir2Stats.put("total", 0);
        dir2Stats.put("minibus", 0);
        dir2Stats.put("truck", 0);
        dir2Stats.put("totalSpeed", 0.0);
        dir2Stats.put("vehicleCount", 0);

        directionStats.put(1, dir1Stats);
        directionStats.put(2, dir2Stats);

        // 遍历所有车道数据
        for (Map.Entry<Integer, Map<String, Double>> entry : laneStats.entrySet()) {
            int lane = entry.getKey();
            Map<String, Double> stats = entry.getValue();

            int busCount = stats.getOrDefault("busCount", 0.0).intValue();
            int trackCount = stats.getOrDefault("trackCount", 0.0).intValue();
            int totalCount = stats.getOrDefault("totalCount", 0.0).intValue();
            double totalSpeed = stats.getOrDefault("totalSpeed", 0.0);
            int vehicleCount = stats.getOrDefault("vehicleCount", 0.0).intValue();

            // 根据车道号判断方向（奇数车道为上行，偶数车道为下行）
            int direction = (lane % 2 == 1) ? 1 : 2;
            Map<String, Object> dirStats = directionStats.get(direction);

            // 累加统计数据
            dirStats.put("total", (int)dirStats.get("total") + totalCount);
            dirStats.put("minibus", (int)dirStats.get("minibus") + busCount);
            dirStats.put("truck", (int)dirStats.get("truck") + trackCount);
            dirStats.put("totalSpeed", (double)dirStats.get("totalSpeed") + totalSpeed);
            dirStats.put("vehicleCount", (int)dirStats.get("vehicleCount") + vehicleCount);
        }

        // 计算平均速度并清理临时字段
        for (int direction : new int[]{1, 2}) {
            Map<String, Object> dirStats = directionStats.get(direction);
            double totalSpeed = (double) dirStats.get("totalSpeed");
            int vehicleCount = (int) dirStats.get("vehicleCount");
            double speed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;

            // 保留两位小数
            speed = Math.round(speed * 100.0) / 100.0;
            dirStats.put("speed", speed);

            // 移除临时字段
            dirStats.remove("totalSpeed");
            dirStats.remove("vehicleCount");
        }

        return directionStats;
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
            stats.put("aveSpeed", aveSpeed);
        }
    }

    // 新增方法：支持按级别统计
    public static Map<String, Object> queryTrafficDataByLevel(String orgcode, long startTime, long endTime, int level) {
        return queryTrafficData(orgcode, startTime, endTime, level);
    }

    // 时间字符串转换方法
    public static long convertToTimestamp(String timeString) {
        try {
            return DATE_FORMAT.parse(timeString).getTime();
        } catch (ParseException e) {
            throw new RuntimeException("时间格式解析错误: " + timeString, e);
        }
    }

    // 修改后的方法，支持级别参数
    public static upDownResult getUpDownChargerByDuration1(String stationId, String startTime, String endTime, int level) {
        long st = convertToTimestamp(startTime);
        long et = convertToTimestamp(endTime);
        Map<String, Object> result = totalOpsV2.queryTrafficDataByLevel(stationId, st, et, level);
        return new upDownResult(200, "查询成功", Arrays.asList(result), true);
    }
}