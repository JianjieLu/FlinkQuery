package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.ljj.flinkquery.demos.entity.CrowdedInfo;
import com.ljj.flinkquery.demos.entity.TrafficEventUtils;
import com.ljj.flinkquery.demos.entity.VehicleSeg;
import com.ljj.flinkquery.demos.entity.newFive.firstResult;
import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.ljj.flinkquery.demos.entity.TrafficEventUtils.*;
import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.web.bind.annotation.RequestParam;

public class totalOps {
    private static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }
// 定义轨迹数据结构
@AllArgsConstructor
@NoArgsConstructor
public static class TrajData {
    private String rowKey;
    private int vehicleType;
    private long latestTime;
    private List<TrajectoryPoint> trajectory;
}

public static List<VehicleData> getVehicleDataInTimeRange(
        long startTime,
        long endTime,
        List<String> qualifiers
) throws IOException {
    List<VehicleData> result = new ArrayList<>();
    Configuration conf = getHBaseConfiguration();

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        // 1. 生成需要查询的表名列表（按日期分表）
        LocalDate startDate = millisToLocalDate(startTime);
        LocalDate endDate = millisToLocalDate(endTime);
        Set<String> tableNames = new HashSet<>();

        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            tableNames.add("ZCarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE));
        }

        // 2. 遍历所有表
        for (String tableName : tableNames) {
            if (!tableExists(connection, tableName)) {
                System.out.println("表不存在，跳过: " + tableName);
                continue;
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName));
                 ResultScanner scanner = getScannerForTable(table, startTime, endTime, qualifiers)) {

                // 3. 处理扫描结果
                Map<String, VehicleData> vehicleMap = new HashMap<>();

                for (Result res : scanner) {
                    String rowKey = Bytes.toString(res.getRow());
                    RowKeyInfo rowKeyInfo = parseRowKey(rowKey);

                    // 4. 过滤时间范围（行键级别）
                    if (!isRowKeyInRange(rowKey, startTime, endTime)) {
                        continue;
                    }

                    // 5. 创建或获取车辆数据对象
                    VehicleData vehicleData = vehicleMap.computeIfAbsent(
                        rowKey,
                        k -> new VehicleData()
                    );

                    vehicleData.setRowKeyInfo(rowKeyInfo);

                    // 6. 处理所有单元格
                    for (Cell cell : res.listCells()) {
                        String family = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));

                        // 只处理指定列（如果指定了列）
                        if (qualifiers != null && !qualifiers.isEmpty() && !qualifiers.contains(qualifier)) {
                            continue;
                        }

                        String value = Bytes.toString(CellUtil.cloneValue(cell));

                        if ("cf0".equals(family)) {
                            switch (qualifier) {
                                case "type":
                                    vehicleData.setType(Integer.parseInt(value));
                                    break;
                                case "latest_time":
                                    vehicleData.setLatestTime(Long.parseLong(value));
                                    break;
                                case "trajectory":
                                    List<TrajectoryPoint> points = parseTrajectory(value);
                                    vehicleData.setTrajectory(points);
                                    break;
                                case "direction":
                                    vehicleData.setDirection(Integer.parseInt(value));
                                    break;

                            }
                        }
                    }
                }

                result.addAll(vehicleMap.values());
            }
        }
    }
    return result;
}
private static ResultScanner getScannerForTable(Table table, long startTime, long endTime, List<String> qualifiers) throws IOException {
    // 创建Scan对象并设置时间范围
    Scan scan = new Scan()
            .withStartRow(Bytes.toBytes(startTime + "-"))
            .withStopRow(Bytes.toBytes((endTime + 1) + "-"));

    // 添加要查询的列
    if (qualifiers != null && !qualifiers.isEmpty()) {
        for (String qualifier : qualifiers) {
            scan.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes(qualifier));
        }
    } else {
        scan.addFamily(Bytes.toBytes("cf0"));
    }

    return table.getScanner(scan);
}
public static List<totalOps.VehicleData> getAllVehicleData(
        String tableName,
        List<String> qualifiers
) throws IOException {
        Configuration conf = getHBaseConfiguration();
        List<totalOps.VehicleData> result = new ArrayList<>();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            if (qualifiers != null && !qualifiers.isEmpty()) {
                for (String qualifier : qualifiers) {
                    scan.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes(qualifier));
                }
            } else {
                scan.addFamily(Bytes.toBytes("cf0"));
            }

            try (ResultScanner scanner = table.getScanner(scan)) {
                Map<String, totalOps.VehicleData> vehicleMap = new HashMap<>();

                for (Result res : scanner) {
                    String rowKey = Bytes.toString(res.getRow());
                    totalOps.RowKeyInfo rowKeyInfo = parseRowKey(rowKey);

                    totalOps.VehicleData vehicleData = vehicleMap.computeIfAbsent(
                        rowKey,
                        k -> new totalOps.VehicleData()
                    );

                    vehicleData.setRowKeyInfo(rowKeyInfo);

                    for (Cell cell : res.listCells()) {
                        String family = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));

                        if ("cf0".equals(family)) {
                            switch (qualifier) {
                                case "type":
                                    vehicleData.setType(Integer.parseInt(value));
                                    break;
                                case "latest_time":
                                    vehicleData.setLatestTime(Long.parseLong(value));
                                    break;
                                case "trajectory":
                                    List<TrajectoryPoint> points = parseTrajectory(value);
                                    vehicleData.setTrajectory(points);
                                    break;
                                case "direction":
                                    vehicleData.setDirection(Integer.parseInt(value));
                                    break;
                            }
                        }
                    }
                }

                result.addAll(vehicleMap.values());
            }
        }
        return result;
    }
public static List<TrajData> getTrajInTimeRange(long startTime, long endTime) throws IOException {
    List<TrajData> resultList = new ArrayList<>();
    Configuration conf = getHBaseConfiguration();

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        LocalDate startDate = millisToLocalDate(startTime);
        LocalDate endDate = millisToLocalDate(endTime);

        // 生成需要查询的表名列表
        Set<String> tableNames = new HashSet<>();
        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            tableNames.add("ZCarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE));
        }
        System.out.println("tableNames:" + tableNames);
        for (String tableName : tableNames) {
            if (!tableExists(connection, tableName)) {
                System.out.println("表不存在: " + tableName);
                continue;
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                Scan scan = new Scan() .withStartRow(Bytes.toBytes(startTime + "-"))
    .withStopRow(Bytes.toBytes((endTime + 1) + "-"));

                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result result : scanner) {
                        // 处理每行结果
                        String rowKey = Bytes.toString(result.getRow());
                        if (!isRowKeyInRange(rowKey, startTime, endTime)) continue;

                        // 解析数据类型
                        int vehicleType = getIntValue(result, "cf0", "type");

                        // 解析最新时间
                        long latestTime = getLongValue(result, "cf0", "latest_time");

                        // 解析轨迹数据
                        List<TrajectoryPoint> trajectoryPoints = parseTrajectory(
                            getStringValue(result, "cf0", "trajectory")
                        );

                        // 添加到结果集
                        resultList.add(new TrajData(rowKey, vehicleType, latestTime, trajectoryPoints));
                    }
                }
            }
        }
    }
    return resultList;
}

// 检查行键中的时间戳是否在查询范围内
private static boolean isRowKeyInRange(String rowKey, long startTime, long endTime) {
    String[] parts = rowKey.split("-");
    if (parts.length > 0) {
        try {
            long rowKeyTime = Long.parseLong(parts[0]);
            return rowKeyTime >= startTime && rowKeyTime <= endTime;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    return false;
}

// 解析轨迹字符串 ([x1,y1,lane1,dir1,speed1], [x2,y2,lane2,dir2,speed2])


// 辅助方法 - 获取单元格的整数值
private static int getIntValue(Result result, String cf, String qualifier) {
    byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
    if (value != null) {
        try {
            return Integer.parseInt(Bytes.toString(value));
        } catch (NumberFormatException e) {
            return -1; // 无效值标记
        }
    }
    return -1;
}

// 辅助方法 - 获取单元格的长整数值
private static long getLongValue(Result result, String cf, String qualifier) {
    byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
    if (value != null) {
        try {
            return Long.parseLong(Bytes.toString(value));
        } catch (NumberFormatException e) {
            return -1; // 无效值标记
        }
    }
    return -1;
}

// 辅助方法 - 获取单元格的字符串值
private static String getStringValue(Result result, String cf, String qualifier) {
    byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
    return (value != null) ? Bytes.toString(value) : "";
}
    private static boolean tableExists(Connection connection, String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    private static LocalDate millisToLocalDate(long millis) {
        return Instant.ofEpochMilli(millis)
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

    public static Map<Integer, Integer> getVehicleTypesByDuration(long startTime, long endTime) {
        Map<Integer, Integer> typeCountMap = new HashMap<>();
        Configuration conf = getHBaseConfiguration();

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            LocalDate startDate = millisToLocalDate(startTime);
            LocalDate endDate = millisToLocalDate(endTime);

            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                String tableName = "CarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE);

                if (!tableExists(connection, tableName)) {
                    System.out.println("表不存在: " + tableName);
                    continue;
                } else {
                    System.out.println("selecting table:  " + tableName);
                }

                try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                    Scan scan = new Scan();
                    scan.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("type"));
                    scan.setCaching(1000);

                    try (ResultScanner scanner = table.getScanner(scan)) {
                        for (Result result : scanner) {
                            byte[] valueBytes = result.getValue(
                                    Bytes.toBytes("cf0"),
                                    Bytes.toBytes("type")
                            );

                            if (valueBytes == null) continue;

                            try {
                                // 直接解析为字符串，然后转换为整数
                                String typeStr = Bytes.toString(valueBytes);
                                int vehicleType = Integer.parseInt(typeStr);

                                // 统计有效类型
                                typeCountMap.put(vehicleType,
                                        typeCountMap.getOrDefault(vehicleType, 0) + 1);
                            } catch (NumberFormatException ex) {
                                // 记录错误日志（可选）
                                System.err.println("无法解析的类型值: " + Bytes.toString(valueBytes));
                                continue;
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("HBase查询异常: " + e.getMessage());
            e.printStackTrace();
        }
        return typeCountMap;
    }

    public static List<Pair<CongestionEvent, Long>> getCongestionEvent(String tableName, List<Long> time) {
        List<Pair<CongestionEvent, Long>> congs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        List<String> ss = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return congs; // 直接返回空列表
            } else {
                System.out.println("查找表 " + tableName);
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                for (long t : time) {
                    Get get = new Get(String.valueOf(t).getBytes());
                    Result result1 = table.get(get);
                    for (Cell cell : result1.rawCells()) {
                        System.out.println("Row Key: " + Bytes.toString(result1.getRow()) + "Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                        JSONArray objects = JSON.parseArray(Bytes.toString(CellUtil.cloneValue(cell)));
                        for (Object object : objects) {
                            congs.add(new Pair<>(JSON.parseObject(object.toString(), CongestionEvent.class), t));
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        System.out.println("congs:" + congs);

        return congs;

    }



    private static RowKeyInfo parseRowKey(String rowKey) {
        // 格式: "时间戳-\xE6\xB9\x98B6P538"
        String[] parts = rowKey.split("-", 2);
        if (parts.length == 2) {
            try {
                long timestamp = Long.parseLong(parts[0]);
                // 处理转义序列（如\xE6\xB9\x98）
                String plateNo = Bytes.toString(Bytes.toBytes(parts[1]));
                return new RowKeyInfo(timestamp, plateNo);
            } catch (Exception e) {
                // 处理解析异常
            }
        }
        return new RowKeyInfo(0L, "未知");
    }
    public static boolean deleteTable(String tableName) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName hbaseTableName = TableName.valueOf(tableName);

            // 检查表是否存在
            if (!admin.tableExists(hbaseTableName)) {
                return false;
            }

            // 先禁用表再删除
            if (admin.isTableEnabled(hbaseTableName)) {
                admin.disableTable(hbaseTableName);
            }

            admin.deleteTable(hbaseTableName);
            return true;
        }
    }
//    public static Map<String, Map<Integer, Integer>> getHourlyTrafficForDayAndPreviousDay(long timeMillis) throws IOException {
//        // 1. 确定日期范围
//        LocalDate targetDate = Instant.ofEpochMilli(timeMillis)
//                                      .atZone(ZoneId.systemDefault())
//                                      .toLocalDate();
//        LocalDate previousDay = targetDate.minusDays(1);
//
//        // 2. 准备结果集：Map<日期, Map<小时, 车流量>>
//        Map<String, Map<Integer, Integer>> result = new LinkedHashMap<>();
//        result.put(targetDate.format(DateTimeFormatter.BASIC_ISO_DATE), new HashMap<>());
//        result.put(previousDay.format(DateTimeFormatter.BASIC_ISO_DATE), new HashMap<>());
//
//        // 3. 初始化每小时计数桶
//        for (Map<Integer, Integer> hourlyCount : result.values()) {
//            for (int hour = 0; hour < 24; hour++) {
//                hourlyCount.put(hour, 0);
//            }
//        }
//
//        Configuration conf = getHBaseConfiguration();
//        try (Connection connection = ConnectionFactory.createConnection(conf)) {
//            // 4. 处理两天数据
//            for (LocalDate date : Arrays.asList(previousDay, targetDate)) {
//                String dateStr = date.format(DateTimeFormatter.BASIC_ISO_DATE);
//                String tableName = "ZCarTraj_" + dateStr;
//
//                if (!tableExists(connection, tableName)) {
//                    System.out.println("跳过不存在的表: " + tableName);
//                    continue;
//                }
//
//                // 5. 获取日期边界
//                long[] dateRange = getDateRange(date);
//                System.out.println("处理日期: " + dateStr + ", 时间范围: " + dateRange[0] + " - " + dateRange[1]);
//
//                try (Table table = connection.getTable(TableName.valueOf(tableName));
//                     ResultScanner scanner = table.getScanner(new Scan())) {
//
//                    // 6. 扫描表中所有车辆
//                    for (Result res : scanner) {
//                        String rowKey = Bytes.toString(res.getRow());
//                        String[] parts = rowKey.split("-");
//                        if (parts.length < 2) continue;
//
//                        // 7. 提取时间戳
//                        long timestamp = Long.parseLong(parts[0]);
//
//                        // 8. 计算小时
//                        ZonedDateTime zdt = Instant.ofEpochMilli(timestamp)
//                                                   .atZone(ZoneId.systemDefault());
//                        int hour = zdt.getHour();
//
//                        // 9. 更新计数
//                        Map<Integer, Integer> hourlyCount = result.get(dateStr);
//                        hourlyCount.put(hour, hourlyCount.get(hour) + 1);
//                    }
//                }
//            }
//        }
//        return result;
//    }




      private static final int SCANNER_CACHING = 10000; // 一次获取10000行
    private static final int THREAD_POOL_SIZE = 2; // 处理两天的数据

    public static Map<String, Map<Integer, Integer>> getHourlyTrafficForDayAndPreviousDay(long timeMillis)
            throws IOException, InterruptedException, ExecutionException {
long st=System.currentTimeMillis();
        // 1. 确定日期范围
        LocalDate targetDate = Instant.ofEpochMilli(timeMillis)
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        LocalDate previousDay = targetDate.minusDays(1);

        // 2. 准备结果集
        Map<String, Map<Integer, Integer>> result = new LinkedHashMap<>();
        result.put(targetDate.format(DateTimeFormatter.BASIC_ISO_DATE), new HashMap<>());
        result.put(previousDay.format(DateTimeFormatter.BASIC_ISO_DATE), new HashMap<>());

        // 初始化小时桶 (0-23)
        for (Map<Integer, Integer> map : result.values()) {
            for (int hour = 0; hour < 24; hour++) {
                map.put(hour, 0);
            }
        }

        // 3. 创建线程池处理两天数据
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<Void>> futures = new ArrayList<>();

        Configuration conf = getHBaseConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 提交两天的处理任务
            for (LocalDate date : Arrays.asList(previousDay, targetDate)) {
                futures.add(executor.submit(new TrafficCounter(connection, date, result)));
            }

            // 等待所有任务完成
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new IOException("并行处理失败", e);
        } finally {
            executor.shutdown();
        }
long st1=System.currentTimeMillis();
long elapsed = st1 - st;  // 计算耗时
    Map<Integer, Integer> timeMap = new HashMap<>();
    timeMap.put(-1, (int) elapsed);  // 特殊键-1存储时间
    result.put("__execution_time__", timeMap);  // 用特殊键放入结果
        return result;
    }

    // 处理单日数据的任务
    private static class TrafficCounter implements Callable<Void> {
        private final Connection connection;
        private final LocalDate date;
        private final Map<String, Map<Integer, Integer>> result;

        public TrafficCounter(Connection connection, LocalDate date,
                              Map<String, Map<Integer, Integer>> result) {
            this.connection = connection;
            this.date = date;
            this.result = result;
        }

        @Override
        public Void call() throws Exception {
            String dateStr = date.format(DateTimeFormatter.BASIC_ISO_DATE);
            String tableName = "ZCarTraj_" + dateStr;

            // 初始化小时计数数组
            int[] hourlyCounts = new int[24];

            // 跳过不存在的表
            if (!tableExists(connection, tableName)) {
                System.out.println("跳过不存在的表: " + tableName);
                return null;
            }

            // 获取日期边界
            long[] dateRange = getDateRange(date);
            long startTime = dateRange[0];
            long endTime = dateRange[1];

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                // 创建优化的Scan对象
                Scan scan = new Scan();
                scan.setCaching(SCANNER_CACHING);
                scan.setCacheBlocks(false);

                // 设置行键范围 (使用时间戳范围)
                scan.setStartRow(Bytes.toBytes(startTime + "-"));
                scan.setStopRow(Bytes.toBytes(endTime + "-"));

                // 只获取行键，不获取列值
                scan.setFilter(new KeyOnlyFilter());

                // 使用高效的ResultScanner
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result res : scanner) {
                        String rowKey = Bytes.toString(res.getRow());
                        String[] parts = rowKey.split("-", 2); // 只分割一次
                        if (parts.length < 1) continue;

                        try {
                            long timestamp = Long.parseLong(parts[0]);
                            ZonedDateTime zdt = Instant.ofEpochMilli(timestamp)
                                    .atZone(ZoneId.systemDefault());
                            int hour = zdt.getHour();

                            // 更新本地计数数组
                            hourlyCounts[hour]++;
                        } catch (NumberFormatException e) {
                            // 忽略格式错误的rowKey
                        }
                    }
                }

                // 批量更新结果
                Map<Integer, Integer> resultMap = result.get(dateStr);
                for (int hour = 0; hour < 24; hour++) {
                    resultMap.put(hour, hourlyCounts[hour]);
                }
            }
            return null;
        }
    }





//    firstResult
//    firstResult
//            firstResult
//    firstResult
//                    firstResult
//    firstResult


//查询指定时间戳当天的车流量
    public static Pair<Integer,Integer> getTodayTotal(long timeMillis) throws IOException {
        long st=System.currentTimeMillis();
        // 1. 确定日期范围
        LocalDate targetDate = Instant.ofEpochMilli(timeMillis)
                                      .atZone(ZoneId.systemDefault())
                                      .toLocalDate();

        // 2. 准备结果集：Map<日期, Map<小时, 车流量>>
        Map<String, Map<Integer, Integer>> result = new LinkedHashMap<>();
        result.put(targetDate.format(DateTimeFormatter.BASIC_ISO_DATE), new HashMap<>());

        // 3. 初始化每小时计数桶
        for (Map<Integer, Integer> hourlyCount : result.values()) {
            for (int hour = 0; hour < 24; hour++) {
                hourlyCount.put(hour, 0);
            }
        }
                String dateStr = targetDate.format(DateTimeFormatter.BASIC_ISO_DATE);

        Configuration conf = getHBaseConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 4. 处理两天数据

                String tableName = "ZCarTraj_" + dateStr;

                if (!tableExists(connection, tableName)) {
                    System.out.println("跳过不存在的表: " + tableName);
                    return new Pair<>(0,0);
                }

                // 5. 获取日期边界
                long[] dateRange = getDateRange(targetDate);
                System.out.println("处理日期: " + dateStr + ", 时间范围: " + dateRange[0] + " - " + dateRange[1]);

                try (Table table = connection.getTable(TableName.valueOf(tableName));
                     ResultScanner scanner = table.getScanner(new Scan())) {

                    // 6. 扫描表中所有车辆
                    for (Result res : scanner) {
                        String rowKey = Bytes.toString(res.getRow());
                        String[] parts = rowKey.split("-");
                        if (parts.length < 2) continue;

                        // 7. 提取时间戳
                        long timestamp = Long.parseLong(parts[0]);

                        // 8. 计算小时
                        ZonedDateTime zdt = Instant.ofEpochMilli(timestamp)
                                                   .atZone(ZoneId.systemDefault());
                        int hour = zdt.getHour();

                        // 9. 更新计数
                        Map<Integer, Integer> hourlyCount = result.get(dateStr);
                        hourlyCount.put(hour, hourlyCount.get(hour) + 1);

                }
            }
        }
        int count = 0;
        for(int i=0;i<24;i++) {
        count+=result.get(dateStr).get(i);

        }
        long st1=System.currentTimeMillis();

        return new Pair<>(count,(int)(st1-st));
    }

    private static long[] getDateRange(LocalDate date) {
        ZonedDateTime start = date.atStartOfDay(ZoneId.systemDefault());
        ZonedDateTime end = start.plusDays(1);

        return new long[] {
            start.toInstant().toEpochMilli(),
            end.toInstant().toEpochMilli() - 1
        };
    }
   public static List<TrajectoryPoint> parseTrajectory(String trajectoryStr) {
        List<TrajectoryPoint> result = new ArrayList<>();

        // 移除字符串首尾的方括号
        String cleaned = trajectoryStr.trim()
            .replaceAll("^\\[", "")
            .replaceAll("]$", "");

        // 按 "), (" 分割各个点
        String[] points = cleaned.split("\\),\\s*\\(");

        for (String point : points) {
            // 移除每个点两端的括号
            String cleanPoint = point.replaceAll("^\\(|\\)$", "");

            // 分割经度、纬度、车道号、方向、速度
            String[] parts = cleanPoint.split(",");

            if (parts.length == 4) {
                double longitude = Double.parseDouble(parts[0].trim());
                double latitude = Double.parseDouble(parts[1].trim());
                int lane = Integer.parseInt(parts[2].trim());
                double speed = Double.parseDouble(parts[3].trim());

                result.add(new TrajectoryPoint(longitude, latitude, lane, speed));
            }
        }
        return result;
    }
    public static List<String> getRowKeysByQualifier(String tableName, String cf, String quali) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        List<String> rowKeys = new ArrayList<>();

        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(quali));
            scan.setMaxVersions(1); // 只获取最新版本

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    // 获取RowKey
                    byte[] rowBytes = result.getRow();
                    String rowKey = Bytes.toString(rowBytes);

                    // 验证是否包含所需列
                    if (result.containsColumn(Bytes.toBytes(cf), Bytes.toBytes(quali))) {
                        rowKeys.add(rowKey);
                    }
                }
            }
        }

        return rowKeys;
    }
    public static List<Integer> getVehicleCountByDirection(String tableName, List<String> rowkeys) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        System.out.println("select table name " + tableName + " keys = " + rowkeys);
        int upTotal = 0;
        int downTotal = 0;

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 1. 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                return Arrays.asList(0, 0); // 表不存在返回空统计
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                // 2. 批量查询
                List<Get> gets = rowkeys.stream()
                        .map(rowkey -> {
                            Get get = new Get(Bytes.toBytes(rowkey));
                            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                            return get;
                        })
                        .collect(Collectors.toList());

                // 3. 获取结果并处理
                for (Result result : table.get(gets)) {
                    if (result.isEmpty()) continue;

                    // 解析车辆数据
                    byte[] valueBytes = result.getValue(
                            Bytes.toBytes("cf"),
                            Bytes.toBytes("VehicleSegments")
                    );

                    JSONArray vehicles = JSON.parseArray(
                            Bytes.toString(valueBytes)
                                    .replace("\\x", "\\u00")
                                    .replace("\\", "\\\\")
                    );

                    // 4. 按方向分类统计
                    for (Object obj : vehicles) {
                        JSONObject vehicle = (JSONObject) obj;
                        int direction = vehicle.getIntValue("direction");

                        if (direction == 1) {
                            upTotal++;
                        } else if (direction == 2) {
                            downTotal++;
                        }
                    }
                }
            }
        } catch (IOException e) {
            // 可根据需要记录日志
            return Arrays.asList(0, 0); // 异常返回空统计
        }

        return Arrays.asList(upTotal, downTotal);
    }

    public static firstResult getNearestMinuteCongestionStats(long timestamp) throws IOException {
    // 1. 计算最近的整分钟时间戳
    long minuteTimestamp = (timestamp / 60000) * 60000;

    // 2. 确定表名（按月对齐）
    String tableName = "CongestionEvents_" +
        Instant.ofEpochMilli(timestamp)
               .atZone(ZoneId.systemDefault())
               .format(DateTimeFormatter.ofPattern("yyyyMM"));

    Configuration conf = getHBaseConfiguration();
    int[] result = new int[4]; // [num1, length1, num2, length2]

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        // 3. 检查表是否存在
        if (!isTableExists(connection, tableName)) {
            System.out.println("表不存在: " + tableName);
            return new firstResult(0,0,0,0); // 返回全0数组
        }

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            // 4. 尝试获取精确匹配的行
            Get get = new Get(Bytes.toBytes(String.valueOf(minuteTimestamp)));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum1"));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength1"));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum2"));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength2"));

            Result resultData = table.get(get);

            // 5. 如果找到精确匹配，直接返回
            if (!resultData.isEmpty()) {
                result[0] = getIntValue(resultData, "cf", "congestionNum1");
                result[1] = getIntValue(resultData, "cf", "congestionLength1");
                result[2] = getIntValue(resultData, "cf", "congestionNum2");
                result[3] = getIntValue(resultData, "cf", "congestionLength2");
            return new firstResult(0,0,0,0); // 返回全0数组
            }

            // 6. 如果没有精确匹配，查找前后最近的行
            long beforeTime = minuteTimestamp - 60000;
            long afterTime = minuteTimestamp + 60000;

            // 尝试获取前一行
            Result beforeResult = table.get(new Get(Bytes.toBytes(String.valueOf(beforeTime)))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum2"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength2")));

            // 尝试获取后一行
            Result afterResult = table.get(new Get(Bytes.toBytes(String.valueOf(afterTime)))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum2"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength2")));

            // 7. 选择最近的有效行
            if (!beforeResult.isEmpty()) {
                result[0] = getIntValue(beforeResult, "cf", "congestionNum1");
                result[1] = getIntValue(beforeResult, "cf", "congestionLength1");
                result[2] = getIntValue(beforeResult, "cf", "congestionNum2");
                result[3] = getIntValue(beforeResult, "cf", "congestionLength2");
            } else if (!afterResult.isEmpty()) {
                result[0] = getIntValue(afterResult, "cf", "congestionNum1");
                result[1] = getIntValue(afterResult, "cf", "congestionLength1");
                result[2] = getIntValue(afterResult, "cf", "congestionNum2");
                result[3] = getIntValue(afterResult, "cf", "congestionLength2");
            }else{
                 beforeTime = minuteTimestamp - 120000;
                 beforeResult = table.get(new Get(Bytes.toBytes(String.valueOf(beforeTime)))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum2"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength2")));
                 if (!beforeResult.isEmpty()) {
                     result[0] = getIntValue(beforeResult, "cf", "congestionNum1");
                result[1] = getIntValue(beforeResult, "cf", "congestionLength1");
                result[2] = getIntValue(beforeResult, "cf", "congestionNum2");
                result[3] = getIntValue(beforeResult, "cf", "congestionLength2");
                 }
            afterTime = minuteTimestamp + 120000;
                  afterResult = table.get(new Get(Bytes.toBytes(String.valueOf(afterTime)))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength1"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionNum2"))
                .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestionLength2")));
                 if (!beforeResult.isEmpty()) {
                     result[0] = getIntValue(afterResult, "cf", "congestionNum1");
                     result[1] = getIntValue(afterResult, "cf", "congestionLength1");
                     result[2] = getIntValue(afterResult, "cf", "congestionNum2");
                     result[3] = getIntValue(afterResult, "cf", "congestionLength2");
                 }

            }
        }
    }

    return new firstResult(result[0],result[2],result[1],result[3]);
}
    public static int[] getOne(List<String> rowkeys) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        int[] result = new int[2];  // result[0]=upSum, result[1]=downSum

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("tab"))) {

            // 构建批量Get请求
            List<Get> gets = new ArrayList<>(rowkeys.size());
            for (String rowkey : rowkeys) {
                Get get = new Get(Bytes.toBytes(rowkey));
                get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("upCount"));
                get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("downCount"));
                gets.add(get);
            }

            // 执行查询
            Result[] results = table.get(gets);

            // 遍历结果累加
            for (Result re : results) {
                if (re.isEmpty()) {
                    System.err.println("Rowkey不存在: " + Bytes.toString(re.getRow()));
                    continue;
                }

                // 处理upCount
                byte[] upBytes = re.getValue(Bytes.toBytes("f1"), Bytes.toBytes("upCount"));
                if (upBytes != null) {
                    try {
                        result[0] += Integer.parseInt(Bytes.toString(upBytes));
                    } catch (NumberFormatException e) {
                        System.err.printf("upCount解析失败: rowkey=%s%n", Bytes.toString(re.getRow()));
                    }
                }

                // 处理downCount
                byte[] downBytes = re.getValue(Bytes.toBytes("f1"), Bytes.toBytes("downCount"));
                if (downBytes != null) {
                    try {
                        result[1] += Integer.parseInt(Bytes.toString(downBytes));
                    } catch (NumberFormatException e) {
                        System.err.printf("downCount解析失败: rowkey=%s%n", Bytes.toString(re.getRow()));
                    }
                }
            }

        }  // 自动关闭连接和表

        return result;
    }
    public static List<Pair<CongestionEvent, Long>> getCongestionEvent1(String tableName, List<Long> time) {

        List<Pair<CongestionEvent, Long>> congs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        boolean a = true;
        int i = 0;
        List<String> ss = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return congs; // 直接返回空列表
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {

                for (long t : time) {
                    Get get = new Get(String.valueOf(t).getBytes());
                    Result result1 = table.get(get);
                    if (a) {
                        for (Cell cell : result1.rawCells()) {
                            System.out.println("Row Key: " + Bytes.toString(result1.getRow()));
                            System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                            System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                            System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                            ss.add("Row Key: " + Bytes.toString(result1.getRow()) + "Value: " + Bytes.toString(CellUtil.cloneValue(cell)) + "Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + "Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                            a = false;
                            try (BufferedWriter writer1 = new BufferedWriter(new FileWriter("/home/ljj/jiaotou/test.txt", true))) {
                                writer1.write("Row Key: " + Bytes.toString(result1.getRow()) + "Value: " + Bytes.toString(CellUtil.cloneValue(cell)) + "Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + "Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                                writer1.write(System.lineSeparator());
                            }
                        }
                    }
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("congestions"));
                    Result result = table.get(get);

                    byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("congestions"));
                    if (valueBytes != null) {
                        String value = Bytes.toString(valueBytes);
                        JSONArray objects = JSON.parseArray(value);
                        for (Object object : objects) {
                            congs.add(new Pair<>(JSON.parseObject(object.toString(), CongestionEvent.class), t));
                            i++;
                        }
                    } else {
                        System.out.println("列 congestions 不存在或值为空");
                    }
                }

            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        System.out.println("congs:" + congs);
        return congs;

    }
    public static void getByRowkey(String tableName, String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("Row Key: " + Bytes.toString(result.getRow()));
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 检查表是否存在的工具方法
    private static boolean isTableExists(Connection connection, String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }
    // cf  VehicleSegments
    public static List<VehicleSeg> getVeByRowkey(String tableName, String rowkey) {
        List<VehicleSeg> vehicleSegs = new ArrayList<>();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            // 检查表是否存在
            if (!isTableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在");
                return vehicleSegs; // 直接返回空列表
            }

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                Get get = new Get(Bytes.toBytes(rowkey));
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                Result result = table.get(get);

                byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
                if (valueBytes != null) {
                    String value = Bytes.toString(valueBytes);
                    JSONArray objects = JSON.parseArray(value);
                    for (Object object : objects) {
                        vehicleSegs.add(JSON.parseObject(object.toString(), VehicleSeg.class));
                    }
                } else {
                    System.out.println("列 cf/VehicleSegments 不存在或值为空");
                }
            }
        } catch (IOException e) {
            System.err.println("HBase操作异常: " + e.getMessage());
            // 可选择记录日志，但不抛出异常
        }
        return vehicleSegs;
    }

    public static List<CrowdedInfo> getCrowdedByRowkey(String tableName, String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        List<CrowdedInfo> crowdedInfos = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowkey));

            // 指定列族和列名
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));
            Result result = table.get(get);
            byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("VehicleSegments"));

            if (valueBytes != null) {
                String value = Bytes.toString(valueBytes);
                JSONArray objects = JSON.parseArray(value);
                for (Object object : objects) {
                    crowdedInfos.add(JSON.parseObject(object.toString(), CrowdedInfo.class));
                }
            } else {
                System.out.println("列 cf 不存在或值为空");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return crowdedInfos;
    }

    public static void createOrDis(Admin admin, String table, HTableDescriptor hTableDescriptor) throws IOException {
        if (admin.tableExists(TableName.valueOf(table))) {
            System.out.println("Table already exists!");
        } else {
            admin.createTable(hTableDescriptor);
        }
    }

    public static void createTable(Configuration conf, String tableName, String cf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf).setCompressionType(Compression.Algorithm.NONE));
            System.out.println("Creating table " + tableName + "...");
            createOrDis(admin, tableName, tableDescriptor);
            System.out.println("Done.");
        }
    }

    public static void deleteTable(Configuration conf, String tableName, String cf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin = connection.getAdmin()) {
            TableName table = TableName.valueOf(tableName);
            //停用表
            admin.disableTable(table);
            //删除列族
            admin.deleteColumn(table, cf.getBytes(StandardCharsets.UTF_8));
            //删除表
            admin.deleteTable(table);
        }
    }

    public static void adadColumnFamily(Configuration conf, String columnFamilyName, String tableName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin = connection.getAdmin();) {
            HColumnDescriptor hcd = new HColumnDescriptor(columnFamilyName);
            admin.addColumn(TableName.valueOf(tableName), hcd);
        }
    }

    public static void deleteByRowkey(Configuration conf, String columnFamilyName, String tableName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(columnFamilyName));
        }
    }

    public static void putLine(Configuration conf, String columnFamilyName, String tableName, String row1, String qualifier, String value) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row1));//1001
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            System.out.println("one put succeed" + value);
            table.put(put);
        }
    }

    public static void putManyLines(Configuration conf,
                                    String columnFamilyName,
                                    String tableName,
                                    String row1,
                                    String qualifier,
                                    String value,
                                    String qualifier2,
                                    String value2) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row1));//1001
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value)).addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier2), Bytes.toBytes(value2));
            System.out.println("two put succeed" + value + "     " + value2);
            table.put(put);
        }

    }

    public static int getDayOfYear(long timestamp) {
        // 1. 将时间戳转换为带时区的日期时间对象
        ZonedDateTime zdt = Instant.ofEpochMilli(timestamp)
                                  .atZone(ZoneId.systemDefault());

        // 2. 获取该日期在一年中的序号（1月1日=1，12月31日=365或366）
        return zdt.getDayOfYear();
    }
  public static int[] getYearToDateTraffic(long timestamp) throws IOException {
    // 1. 确定时间范围
    ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    int year = dateTime.getYear();
    long startOfYear = LocalDate.of(year, 1, 1)
                                .atStartOfDay(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
    long endOfQuery = timestamp + 1; // 查询结束时间（开区间）

    // 2. 预聚合数据结构
    int upTotal = 0;
    int downTotal = 0;
    Configuration conf = getHBaseConfiguration();

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        // 3. 获取所有需要查询的表
        LocalDate startDate = LocalDate.of(year, 1, 1);
        LocalDate endDate = dateTime.toLocalDate();
        List<TableName> tablesToScan = new ArrayList<>();

        // 使用Admin批量检查表是否存在
        try (Admin admin = connection.getAdmin()) {
            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                String tableName = "ZCarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE);
                TableName tn = TableName.valueOf(tableName);
                if (admin.tableExists(tn)) {
                    tablesToScan.add(tn);
                } else {
                    System.out.println("表不存在，跳过: " + tableName);
                }
            }
        }

        // 4. 创建线程池并行处理表扫描
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(tablesToScan.size(), 10));
        List<Future<int[]>> futures = new ArrayList<>();

        for (TableName tableName : tablesToScan) {
            futures.add(executor.submit(() -> {
                int upCount = 0;
                int downCount = 0;

                try (Table table = connection.getTable(tableName)) {
                    Scan scan = new Scan();
                    scan.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("direction"));

                    // 关键优化：设置精确的RowKey范围
                    if (tableName.getNameAsString().endsWith(endDate.format(DateTimeFormatter.BASIC_ISO_DATE))) {
                        // 最后一天：精确时间范围
                        scan.setStartRow(Bytes.toBytes(startOfYear + "-"));
                        scan.setStopRow(Bytes.toBytes(endOfQuery + "-"));
                    } else {
                        // 其他天：整表扫描（因为都是当年数据）
                        scan.setStartRow(Bytes.toBytes(startOfYear + "-"));
                    }

                    scan.setCaching(1000); // 批量获取
                    scan.setBatch(100);    // 每批列数

                    try (ResultScanner scanner = table.getScanner(scan)) {
                        for (Result result : scanner) {
                            byte[] valueBytes = result.getValue(
                                Bytes.toBytes("cf0"),
                                Bytes.toBytes("direction")
                            );

                            if (valueBytes != null) {
                                int direction = Integer.parseInt(Bytes.toString(valueBytes));
                                if (direction == 1) upCount++;
                                else if (direction == 2) downCount++;
                            }
                        }
                    }
                }
                return new int[]{upCount, downCount};
            }));
        }

        // 5. 汇总结果
        for (Future<int[]> future : futures) {
            int[] counts = future.get();
            upTotal += counts[0];
            downTotal += counts[1];
        }

        executor.shutdown();
    } catch (InterruptedException | ExecutionException e) {
        throw new IOException("多线程处理失败", e);
    }

    return new int[]{upTotal, downTotal};
}
    @Data
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TrajectoryPoint {
        private double longitude;
        private double latitude;
        private int lane;
        private double speed;
    }

    @Data
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RowKeyInfo {
        private long timestamp;
        private String plateNo;
    }

    @Data
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleData {
        private RowKeyInfo rowKeyInfo;
        private Integer type;
        private Long latestTime;
        private List<TrajectoryPoint> trajectory;
        private int direction;
    }
}