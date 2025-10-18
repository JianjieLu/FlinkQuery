package com.ljj.flinkquery.demos.web.impl.edu.querys;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TrafficStatsQuery {

    private static final String COLUMN_FAMILY = "stats";
    private static final String BUS_COUNT_COL = "bus_count";
    private static final String TRUCK_COUNT_COL = "truck_count";
    private static final String OTHER_COUNT_COL = "other_count";
    private static final SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyyMMddHH");
    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyyMMdd");

    public static Pair<List<List<Integer>>, List<String>> queryTrafficStats(
            String tableName,
            long stt,
            long ett,
            int startStake,
            int endStake) {

        List<List<Integer>> result = new ArrayList<>();

        // 创建HBase配置和连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 获取时间范围内的所有小时
        List<String> hours = getHourRange(stt, ett);
        if (hours.isEmpty()) {
            return new Pair<>(result, null);
        }
        String startHour = hours.get(0);
        String endHour = hours.get(hours.size() - 1);

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {

            // 1. 创建扫描器
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(COLUMN_FAMILY));

            // 2. 设置行键范围过滤器
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            // 桩号范围过滤器（新格式：桩号在前）
            // 起始行键：最小桩号_最小时间戳_最小方向
            String startRowKey = "K" + startStake + "_" + startHour + "_1";
            // 结束行键：最大桩号_最大时间戳_最大方向
            String endRowKey = "K" + endStake + "_" + endHour + "_2";

            // 行键范围过滤器
            RowFilter startFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(startRowKey)));
            filterList.addFilter(startFilter);

            RowFilter endFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(endRowKey)));
            filterList.addFilter(endFilter);

            // 桩号精确范围过滤器（确保只扫描指定桩号）
            RegexStringComparator regex = new RegexStringComparator(
                    "K(" + startStake + "|" + (startStake + 1) + "|...|" + endStake + ")_\\d{10}_[12]");
            RowFilter stakeFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, regex);
            filterList.addFilter(stakeFilter);

            scan.setFilter(filterList);

            // 3. 执行扫描并处理结果
            ResultScanner scanner = table.getScanner(scan);

            // 按小时和方向分组统计
            // Map<小时, Map<方向, [客车数, 货车数]>>
            Map<String, Map<Integer, int[]>> hourlyStats = new TreeMap<>();

            // 初始化所有小时和方向的数据
            for (String hour : hours) {
                Map<Integer, int[]> directionMap = new HashMap<>();
                directionMap.put(1, new int[]{0, 0}); // 上行方向
                directionMap.put(2, new int[]{0, 0}); // 下行方向
                hourlyStats.put(hour, directionMap);
            }

            for (Result re : scanner) {
                String rowKey = Bytes.toString(re.getRow());
                String[] parts = rowKey.split("_");

                if (parts.length < 3) continue; // 无效行键

                // 新格式：桩号_时间戳_方向
                String stake = parts[0];      // 桩号（如K1234）
                String hour = parts[1];       // 时间戳（yyyyMMddHH格式）
                int direction = Integer.parseInt(parts[2]); // 方向（1或2）

                // 只处理在时间范围内的数据
                if (!hourlyStats.containsKey(hour)) continue;

                // 解析桩号（去掉"K"前缀）
                int stakeNum = Integer.parseInt(stake.substring(1));
                if (stakeNum < startStake || stakeNum > endStake) continue;

                // 获取列值
                int busCount = getIntValue(re, BUS_COUNT_COL);
                int truckCount = getIntValue(re, TRUCK_COUNT_COL);

                // 累加到小时和方向统计
                Map<Integer, int[]> directionMap = hourlyStats.get(hour);
                int[] stats = directionMap.get(direction);
                stats[0] += busCount;     // 客车数
                stats[1] += truckCount;   // 货车数
            }

            // 4. 按小时顺序组织结果
            for (String hour : hours) {
                Map<Integer, int[]> directionMap = hourlyStats.get(hour);

                // 上行方向（1）
                int[] upStats = directionMap.get(1);
                int upBusCount = upStats[0];
                int upTruckCount = upStats[1];
                int upTotal = upBusCount + upTruckCount;

                // 下行方向（2）
                int[] downStats = directionMap.get(2);
                int downBusCount = downStats[0];
                int downTruckCount = downStats[1];
                int downTotal = downBusCount + downTruckCount;

                // 创建小时结果列表：上行客货总数, 上行客车数, 上行货车数, 下行客货总数, 下行客车数, 下行货车数
                List<Integer> hourStats = new ArrayList<>();
                hourStats.add(upTotal);      // 上行客货总数
                hourStats.add(upBusCount);    // 上行客车数
                hourStats.add(upTruckCount);  // 上行货车数
                hourStats.add(downTotal);     // 下行客货总数
                hourStats.add(downBusCount);  // 下行客车数
                hourStats.add(downTruckCount);// 下行货车数

                result.add(hourStats);
            }

        } catch (IOException e) {
            System.err.println("HBase查询异常: " + e.getMessage());
            e.printStackTrace();
        }

        return new Pair<>(result, hours);
    }

    /**
     * 按天聚合小时数据
     * @param hourlyStats 小时统计数据（每个元素是包含6个整数的列表）
     * @param hours 对应的小时字符串列表（yyyyMMddHH格式）
     * @return 按天聚合的结果列表（每个元素是包含6个整数的列表）
     */
    public static List<List<Integer>> aggregateDailyStats(
            List<List<Integer>> hourlyStats,
            List<String> hours) {

        // 按天分组聚合
        Map<String, int[]> dailyAggregates = new TreeMap<>();

        for (int i = 0; i < hourlyStats.size(); i++) {
            List<Integer> hourData = hourlyStats.get(i);
            String hour = hours.get(i);
            String day = hour.substring(0, 8); // 提取日期部分

            // 获取或创建当天的聚合数组
            int[] dayStats = dailyAggregates.computeIfAbsent(day, k -> new int[6]);

            // 累加小时数据到天数据
            for (int j = 0; j < 6; j++) {
                dayStats[j] += hourData.get(j);
            }
        }

        // 转换为结果列表
        List<List<Integer>> dailyStats = new ArrayList<>();
        for (Map.Entry<String, int[]> entry : dailyAggregates.entrySet()) {
            List<Integer> dayList = new ArrayList<>();
            for (int value : entry.getValue()) {
                dayList.add(value);
            }
            dailyStats.add(dayList);
        }

        return dailyStats;
    }

    /**
     * 从Result中获取整数值
     */
    private static int getIntValue(Result result, String column) {
        byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(column));
        if (value != null) {
            try {
                return Integer.parseInt(Bytes.toString(value));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    /**
     * 将时间戳转换为小时字符串（yyyyMMddHH格式）
     * @param timestamp 时间戳（毫秒）
     * @return 小时字符串
     */
    public static String toHourString(long timestamp) {
        return HOUR_FORMAT.format(new Date(timestamp));
    }

    /**
     * 将时间戳转换为日期字符串（yyyyMMdd格式）
     * @param timestamp 时间戳（毫秒）
     * @return 日期字符串
     */
    public static String toDayString(long timestamp) {
        return DAY_FORMAT.format(new Date(timestamp));
    }

    /**
     * 将时间戳向前取整到整点小时
     * @param timestamp 时间戳（毫秒）
     * @return 取整后的时间戳
     */
    public static long floorToHour(long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    /**
     * 将时间戳向后取整到整点小时
     * @param timestamp 时间戳（毫秒）
     * @return 取整后的时间戳
     */
    public static long ceilToHour(long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        return cal.getTimeInMillis();
    }

    /**
     * 获取时间范围内的所有小时字符串
     * @param startTimestamp 起始时间戳（毫秒）
     * @param endTimestamp 结束时间戳（毫秒）
     * @return 小时字符串列表（yyyyMMddHH格式）
     */
   public static List<String> getHourRange(long startTimestamp, long endTimestamp) {
    List<String> hours = new ArrayList<>();

    // 使用精确的时间范围，不进行ceil取整
    long start = floorToHour(startTimestamp);
    long end = endTimestamp; // 不进行ceil，使用原始结束时间

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(start);

    while (cal.getTimeInMillis() <= end) {
        hours.add(toHourString(cal.getTimeInMillis()));
        cal.add(Calendar.HOUR_OF_DAY, 1);
    }

    return hours;
}

    /**
     * 获取时间范围内的所有日期字符串
     * @param startTimestamp 起始时间戳（毫秒）
     * @param endTimestamp 结束时间戳（毫秒）
     * @return 日期字符串列表（yyyyMMdd格式）
     */
    public static List<String> getDayRange(long startTimestamp, long endTimestamp) {
        List<String> days = new ArrayList<>();

        // 向前取整起始时间到当天开始
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(startTimestamp);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long start = cal.getTimeInMillis();

        // 向后取整结束时间到当天结束
        cal.setTimeInMillis(endTimestamp);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        long end = cal.getTimeInMillis();

        cal.setTimeInMillis(start);

        while (cal.getTimeInMillis() <= end) {
            days.add(toDayString(cal.getTimeInMillis()));
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        return days;
    }
}