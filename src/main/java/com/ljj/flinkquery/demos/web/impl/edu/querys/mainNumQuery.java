package com.ljj.flinkquery.demos.web.impl.edu.querys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class mainNumQuery {

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";
    private static final String HBASE_ZOOKEEPER_PORT = "2181";

    // 表名和列族
    private static final String HOURLY_TABLE = "AnaHourlyTrafficFlow";
    private static final String DAILY_TABLE = "AnaDailyTrafficFlow";
    private static final String MONTHLY_TABLE = "AnaMonthlyTrafficFlow";
    private static final String COLUMN_FAMILY = "cf";

    // 时间格式
    private static final DateTimeFormatter HOURLY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHH");
    private static final DateTimeFormatter DAILY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter MONTHLY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    private static final DateTimeFormatter OUTPUT_HOURLY_FORMATTER = DateTimeFormatter.ofPattern("H:mm");
    private static final DateTimeFormatter OUTPUT_DAILY_FORMATTER = DateTimeFormatter.ofPattern("MM-dd");
    private static final DateTimeFormatter OUTPUT_MONTHLY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");

    public static void main(String[] args) {
        // 示例调用
        String startTime = "2025-09-01 00:00:00";
        String endTime = "2025-09-01 23:59:59";
        String startStake = "K1234+001";
        String endStake = "K1235+020";
        int level = 1; // 1:小时, 2:天, 3:月

        try {
            String result = queryMainRoadTrafficData(startTime, endTime, startStake, endStake, level);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询主路交通数据
     * @param startTime 起始时间 (yyyy-MM-dd HH:mm:ss)
     * @param endTime 结束时间 (yyyy-MM-dd HH:mm:ss)
     * @param startStake 起始桩号
     * @param endStake 结束桩号
     * @param level 时间粒度 (1:小时, 2:天, 3:月)
     * @return JSON格式的查询结果
     */
    public static String queryMainRoadTrafficData(String startTime, String endTime,
                                                 String startStake, String endStake, int level) throws Exception {
        // 解析时间参数
        LocalDateTime start = parseDateTime(startTime);
        LocalDateTime end = parseDateTime(endTime);

        // 确定表名和时间格式
        String tableName;
        DateTimeFormatter formatter;
        DateTimeFormatter outputFormatter;
        ChronoUnit timeUnit;

        switch (level) {
            case 1:
                tableName = HOURLY_TABLE;
                formatter = HOURLY_FORMATTER;
                outputFormatter = OUTPUT_HOURLY_FORMATTER;
                timeUnit = ChronoUnit.HOURS;
                break;
            case 2:
                tableName = DAILY_TABLE;
                formatter = DAILY_FORMATTER;
                outputFormatter = OUTPUT_DAILY_FORMATTER;
                timeUnit = ChronoUnit.DAYS;
                break;
            case 3:
                tableName = MONTHLY_TABLE;
                formatter = MONTHLY_FORMATTER;
                outputFormatter = OUTPUT_MONTHLY_FORMATTER;
                timeUnit = ChronoUnit.MONTHS;
                break;
            default:
                throw new IllegalArgumentException("不支持的level参数: " + level);
        }

        // 计算桩号范围对应的sectionId
        int startSectionId = calculateSectionId(startStake);
        int endSectionId = calculateSectionId(endStake);

        if (startSectionId > endSectionId) {
            throw new IllegalArgumentException("起始桩号不能大于结束桩号");
        }

        // 生成时间序列
        List<LocalDateTime> timePoints = generateTimePoints(start, end, timeUnit);

        // 查询HBase数据
        JSONArray resultArray = new JSONArray();

        try (Connection connection = createHBaseConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {

            // 按时间点查询
            for (LocalDateTime timePoint : timePoints) {
                String timeStr = timePoint.format(formatter);

                // 初始化该时间点的统计数据
                long total = 0;
                long minibus = 0;
                long truck = 0;

                // 查询该时间点所有相关section的数据
                for (int sectionId = startSectionId; sectionId <= endSectionId; sectionId++) {
                    String rowKeyPrefix = timeStr + "-" + sectionId;

                    // 使用前缀过滤器查询该section的数据
                    Scan scan = new Scan();
                    scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKeyPrefix)));

                    try (ResultScanner scanner = table.getScanner(scan)) {
                        for (Result result : scanner) {
                            // 获取各列数据
                            byte[] upBusBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("up_bus"));
                            byte[] upTruckBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("up_truck"));
                            byte[] downBusBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("down_bus"));
                            byte[] downTruckBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("down_truck"));

                            // 累加数据
                            if (upBusBytes != null) minibus += Long.parseLong(Bytes.toString(upBusBytes));
                            if (upTruckBytes != null) truck += Long.parseLong(Bytes.toString(upTruckBytes));
                            if (downBusBytes != null) minibus += Long.parseLong(Bytes.toString(downBusBytes));
                            if (downTruckBytes != null) truck += Long.parseLong(Bytes.toString(downTruckBytes));
                        }
                    }
                }

                // 计算总数
                total = minibus + truck;

                // 构建结果对象
                JSONObject timeResult = new JSONObject();
                timeResult.put("time", timePoint.format(outputFormatter));
                timeResult.put("total", total);
                timeResult.put("minibus", minibus);
                timeResult.put("truck", truck);

                resultArray.put(timeResult);
            }
        }

        // 构建最终结果
        JSONObject finalResult = new JSONObject();
        finalResult.put("staList", resultArray);

        return finalResult.toString();
    }

    /**
     * 计算桩号对应的sectionId
     */
    private static int calculateSectionId(String stake) {
        try {
            // 提取桩号数字部分
            String cleanStake = stake.replace("K", "").replace("+", "");
            double mileage = Double.parseDouble(cleanStake) / 1000.0;
            return (int) Math.floor(mileage / 10);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无效的桩号格式: " + stake);
        }
    }

    /**
     * 创建HBase连接
     */
    private static Connection createHBaseConnection() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT);
        return ConnectionFactory.createConnection(config);
    }

    /**
     * 解析时间字符串
     */
    private static LocalDateTime parseDateTime(String timeStr) {
        return LocalDateTime.parse(
            timeStr,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        );
    }

    /**
     * 生成时间点序列
     */
    private static List<LocalDateTime> generateTimePoints(LocalDateTime start, LocalDateTime end, ChronoUnit unit) {
        List<LocalDateTime> timePoints = new ArrayList<>();
        LocalDateTime current = start;

        while (!current.isAfter(end)) {
            timePoints.add(current);
            current = current.plus(1, unit);
        }

        return timePoints;
    }
}