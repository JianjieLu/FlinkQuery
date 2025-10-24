package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import com.ljj.flinkquery.demos.entity.SectionalFlowData;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class totalOpsv3 {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    public static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    /**
     * 查询交通统计数据
     * @param startTime 起始时间(yyyy-MM-dd HH:mm:ss格式)
     * @param endTime 结束时间(yyyy-MM-dd HH:mm:ss格式)
     * @param startStake 起始桩号(K1234+001格式)
     * @param endStake 结束桩号(K1235+020格式)
     * @param level 聚合级别(1=小时, 2=天, 3=月)
     * @param name 路段名称
     * @return 交通统计结果实体类
     */
    public static TrafficResult queryTrafficStats(String startTime, String endTime,
                                              String startStake, String endStake, int level, String name) throws IOException {
        // 1. 转换时间格式
        String formattedStartTime = convertTimeFormat(startTime);
        String formattedEndTime = convertTimeFormat(endTime);

        // 2. 提取桩号数字部分
        int startStakeNum = extractStakeNumber(startStake);
        int endStakeNum = extractStakeNumber(endStake);

        // 3. 构建桩号范围字符串
        String stakeRange = startStake + " - " + endStake;

        // 4. 构建桩号键列表
        List<String> stakeKeys = generateStakeKeys(startStakeNum, endStakeNum);

        // 5. 构建RowKey范围
        List<RowKeyRange> rowKeyRanges = generateRowKeyRanges(stakeKeys, formattedStartTime, formattedEndTime);
        System.out.println(rowKeyRanges);

        // 6. 从HBase查询数据
        List<TrafficRecord> records = scanTrafficData(rowKeyRanges);

        // 7. 按级别聚合数据
        Map<String, Map<Integer, TrafficStats>> aggregatedData = aggregateData(records, level);

        // 8. 构建返回结果
        TrafficResult result = buildResult(aggregatedData, level);
        result.setStake(stakeRange);
        result.setName(name);
        return result;
    }

    /**
     * 转换时间格式
     * 从 "yyyy-MM-dd HH:mm:ss" 转换为 "yyyyMMddHH"
     */
    private static String convertTimeFormat(String timeStr) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
        LocalDateTime dateTime = LocalDateTime.parse(timeStr, inputFormatter);
        return dateTime.format(outputFormatter);
    }

    /**
     * 从桩号字符串中提取整数部分
     * 例如: "K1234+001" -> 1234
     */
    private static int extractStakeNumber(String stakeStr) {
        Pattern pattern = Pattern.compile("K(\\d+)\\+\\d+");
        Matcher matcher = pattern.matcher(stakeStr);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 0;
    }

    /**
     * 生成桩号键列表
     */
    private static List<String> generateStakeKeys(int startStake, int endStake) {
        List<String> stakeKeys = new ArrayList<>();
        for (int stake = startStake; stake <= endStake; stake++) {
            stakeKeys.add("K" + stake);
        }
        return stakeKeys;
    }

    /**
     * 生成RowKey范围列表（桩号在前格式）
     */
    private static List<RowKeyRange> generateRowKeyRanges(List<String> stakeKeys, String startTime, String endTime) {
        List<RowKeyRange> ranges = new ArrayList<>();
        for (String stakeKey : stakeKeys) {
            for (int direction = 1; direction <= 2; direction++) {
                // 桩号在前格式：桩号_时间戳_方向
                String startRowKey = stakeKey + "_" + startTime + "_" + direction;
                String endRowKey = stakeKey + "_" + endTime + "_" + direction;
                ranges.add(new RowKeyRange(startRowKey, endRowKey));
            }
        }
        return ranges;
    }

    /**
     * 扫描HBase获取交通数据
     */
    private static List<TrafficRecord> scanTrafficData(List<RowKeyRange> rowKeyRanges) throws IOException {
        List<TrafficRecord> records = new ArrayList<>();
        Configuration conf = getHBaseConfiguration();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("traffic_stats_by_section"))) {

            for (RowKeyRange range : rowKeyRanges) {
                Scan scan = new Scan();
                scan.withStartRow(Bytes.toBytes(range.startRowKey));
                scan.withStopRow(Bytes.toBytes(range.endRowKey));
                scan.addFamily(Bytes.toBytes("stats"));
                System.out.println("range.startRowKey:"+range.startRowKey+"  endRowKey:"+range.endRowKey);

                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result result : scanner) {
                        TrafficRecord record = parseResult(result);
                        if (record != null) {
                            records.add(record);
                            System.out.println(record.toString());
                        }
                    }
                }
            }
        }

        return records;
    }

    /**
     * 解析HBase查询结果（桩号在前格式）
     */
    private static TrafficRecord parseResult(Result result) {
        String rowKey = Bytes.toString(result.getRow());
        String[] parts = rowKey.split("_");
        if (parts.length != 3) return null;

        // 新格式：桩号_时间戳_方向
        String stakeKey = parts[0];
        String timeKey = parts[1];
        int direction = Integer.parseInt(parts[2]);

        // 获取各列的值
        int busCount = getIntValue(result, "stats", "bus_count");
        int truckCount = getIntValue(result, "stats", "truck_count");
        int total = busCount + truckCount;

        return new TrafficRecord(timeKey, stakeKey, direction, total, busCount, truckCount);
    }

    /**
     * 从Result中获取整数值
     */
    private static int getIntValue(Result result, String family, String qualifier) {
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return value != null ? Integer.parseInt(Bytes.toString(value)) : 0;
    }

    /**
     * 按级别聚合数据
     */
    private static Map<String, Map<Integer, TrafficStats>> aggregateData(List<TrafficRecord> records, int level) {
        Map<String, Map<Integer, TrafficStats>> result = new HashMap<>();

        for (TrafficRecord record : records) {
            // 根据聚合级别转换时间键
            String timeKey = convertTimeKey(record.timeKey, level);

            // 获取或创建方向统计Map
            Map<Integer, TrafficStats> directionMap = result.computeIfAbsent(timeKey, k -> new HashMap<>());

            // 获取或创建该方向的统计对象
            TrafficStats stats = directionMap.computeIfAbsent(record.direction, k -> new TrafficStats());

            // 累加统计值
            stats.total += record.total;
            stats.busCount += record.busCount;
            stats.truckCount += record.truckCount;
            System.out.println("total:"+record.total+" busCount:"+record.busCount+" truckCount:"+record.truckCount);
        }
        System.out.println("=========================a end =======================");
        return result;
    }

    /**
     * 根据聚合级别转换时间键
     */
    private static String convertTimeKey(String timeKey, int level) {
        if (level == 1) {
            // 小时级别，返回HH:00格式
            return timeKey.substring(8, 10) + ":00";
        } else if (level == 2) {
            // 天级别，返回日期部分
            return timeKey.substring(0, 8);
        } else if (level == 3) {
            // 月级别，返回年月部分
            return timeKey.substring(0, 6);
        }
        return timeKey;
    }

    /**
     * 构建返回结果实体类
     */
    private static TrafficResult buildResult(Map<String, Map<Integer, TrafficStats>> aggregatedData, int level) {
        TrafficResult result = new TrafficResult();
        List<DirectionStats> dirStaList = new ArrayList<>();

        // 为两个方向创建结果
        for (int direction = 1; direction <= 2; direction++) {
            DirectionStats dirStats = new DirectionStats();
            dirStats.setDirection(direction);

            List<TimeStats> staList = new ArrayList<>();

            // 获取所有时间键并排序
            List<String> timeKeys = new ArrayList<>(aggregatedData.keySet());
            timeKeys.sort(String::compareTo);

            for (String timeKey : timeKeys) {
                Map<Integer, TrafficStats> directionMap = aggregatedData.get(timeKey);
                TrafficStats stats = directionMap.get(direction);

                if (stats != null) {
                    TimeStats timeStats = new TimeStats();

                    // 设置时间显示格式
                    if (level == 1) {
                        timeStats.setTime(timeKey); // 已经是HH:00格式
                    } else {
                        timeStats.setTime("00:00"); // 天和月级别显示00:00
                    }

                    timeStats.setTotal(stats.total);
                    timeStats.setMinibus(stats.busCount);
                    timeStats.setTruck(stats.truckCount);

                    staList.add(timeStats);
                }
            }

            dirStats.setStaList(staList);
            dirStaList.add(dirStats);
        }

        result.setDirStaList(dirStaList);
        return result;
    }

    /**
     * 获取当前时间段和上一时间段的交通统计数据
     * @param startTime 起始时间(yyyy-MM-dd HH:mm:ss格式)
     * @param endTime 结束时间(yyyy-MM-dd HH:mm:ss格式)
     * @param startStake 起始桩号(K1234+001格式)
     * @param endStake 结束桩号(K1235+020格式)
     * @param name 路段名称
     * @return 包含当前和上一时间段交通统计数据的对象
     */
    public static PeriodTrafficCounts getTrafficCountWithPreviousPeriod(
            String startTime, String endTime,
            String startStake, String endStake, String name) throws IOException {

        // 1. 计算时间段长度
        Duration duration = calculateDuration(startTime, endTime);

        // 2. 计算上一时间段的起止时间
        LocalDateTime startDateTime = LocalDateTime.parse(startTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime endDateTime = LocalDateTime.parse(endTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        LocalDateTime previousStartTime = startDateTime.minus(duration);
        LocalDateTime previousEndTime = endDateTime.minus(duration);

        String previousStartTimeStr = previousStartTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String previousEndTimeStr = previousEndTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // 3. 查询当前时间段的数据
        TrafficCounts currentCounts = queryTotalCounts(startTime, endTime, startStake, endStake);

        // 4. 查询上一时间段的数据
        TrafficCounts previousCounts = queryTotalCounts(previousStartTimeStr, previousEndTimeStr, startStake, endStake);

        // 5. 构建返回结果
        return new PeriodTrafficCounts(
                currentCounts.direction1, currentCounts.direction2,
                previousCounts.direction1, previousCounts.direction2,
                name, startStake + "-" + endStake
        );
    }

    /**
     * 计算时间段长度
     */
    private static Duration calculateDuration(String startTime, String endTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime start = LocalDateTime.parse(startTime, formatter);
        LocalDateTime end = LocalDateTime.parse(endTime, formatter);
        return Duration.between(start, end);
    }

    /**
     * 查询指定时间段和桩号范围内的交通统计数据
     */
    private static TrafficCounts queryTotalCounts(String startTime, String endTime,
                                                 String startStake, String endStake) throws IOException {
        // 1. 转换时间格式
        String formattedStartTime = convertTimeFormat(startTime);
        String formattedEndTime = convertTimeFormat(endTime);

        // 2. 提取桩号数字部分
        int startStakeNum = extractStakeNumber(startStake);
        int endStakeNum = extractStakeNumber(endStake);

        // 3. 构建桩号键列表
        List<String> stakeKeys = generateStakeKeys(startStakeNum, endStakeNum);

        // 4. 构建RowKey范围（桩号在前格式）
        List<RowKeyRange> rowKeyRanges = generateRowKeyRanges(stakeKeys, formattedStartTime, formattedEndTime);

        // 5. 从HBase查询数据
        List<TrafficRecord> records = scanTrafficData(rowKeyRanges);

        // 6. 计算两个方向的总车辆数
        int direction1Total = 0;
        int direction2Total = 0;

        for (TrafficRecord record : records) {
            if (record.direction == 1) {
                direction1Total += record.total;
            } else if (record.direction == 2) {
                direction2Total += record.total;
            }
        }

        return new TrafficCounts(direction1Total, direction2Total);
    }

    // ==================== 实体类定义 ====================

    /**
     * 时间段交通统计数据实体类
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Data
    public static class PeriodTrafficCounts {
        private int currentDirection1; // 当前时间段方向1总车辆数
        private int currentDirection2; // 当前时间段方向2总车辆数
        private int previousDirection1; // 上一时间段方向1总车辆数
        private int previousDirection2; // 上一时间段方向2总车辆数
        private String name; // 路段名称
        private String stake;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class noPiece {
        private int direction;
        private int total;
        private int yoy;
        private int mom;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class noData {
        private String name;
        private String stake;
        private List<noPiece> dirList;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class noResult {
        private int code;
        private String message;
        private noData data;
        private boolean status;
    }

    /**
     * 交通统计数据实体类
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    private static class TrafficCounts {
        private int direction1; // 方向1总车辆数
        private int direction2; // 方向2总车辆数
    }

    /**
     * 内部类：RowKey范围
     */
    @Data
    @ToString
    private static class RowKeyRange {
        String startRowKey;
        String endRowKey;

        RowKeyRange(String startRowKey, String endRowKey) {
            this.startRowKey = startRowKey;
            this.endRowKey = endRowKey;
        }
    }

    /**
     * 内部类：交通记录
     */
    @Data
    @ToString
    private static class TrafficRecord {
        String timeKey;
        String stakeKey;
        int direction;
        int total;
        int busCount;
        int truckCount;

        TrafficRecord(String timeKey, String stakeKey, int direction, int total, int busCount, int truckCount) {
            this.timeKey = timeKey;
            this.stakeKey = stakeKey;
            this.direction = direction;
            this.total = total;
            this.busCount = busCount;
            this.truckCount = truckCount;
        }
    }

    /**
     * 内部类：交通统计数据
     */
    private static class TrafficStats {
        int total = 0;
        int busCount = 0;
        int truckCount = 0;
    }

    /**
     * 检查表是否存在
     */
    private static boolean tableExists(Connection connection, String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    /**
     * 交通统计结果实体类
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TrafficResult {
        private List<DirectionStats> dirStaList;
        private String stake; // 查询的桩号范围
        private String name; // 查询的路段名称
    }

    /**
     * 方向统计实体类
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class DirectionStats {
        private int direction;
        private List<TimeStats> staList;
    }

    /**
     * 时间统计实体类
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TimeStats {
        private String time;
        private int total;
        private int minibus;
        private int truck;
    }
}