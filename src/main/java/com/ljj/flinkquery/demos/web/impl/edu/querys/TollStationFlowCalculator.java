package com.ljj.flinkquery.demos.web.impl.edu.querys;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ljj.flinkquery.demos.entity.data.seventhData;
import com.ljj.flinkquery.demos.entity.data.seventhPiece;
import com.ljj.flinkquery.demos.entity.data.seventhResult;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TollStationFlowCalculator {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String TABLE_NAME = "tabl";
    private static final String COLUMN_FAMILY = "f1";

    private Configuration conf;
    private Connection connection;

    // 收费站配置
    private Map<String, String> stationConfig = new HashMap<>();
    private Map<String, String> stationConfig1 = new HashMap<>();

    public TollStationFlowCalculator() {
        // 初始化HBase配置
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 初始化收费站配置
        initializeStationConfig();
    }

    private void initializeStationConfig() {
        // 收费站名称映射到orgcode
        stationConfig.put("孝感收费站", "C7370151-2116-470A-8E26-5F878B3C9D78");
        stationConfig.put("XG01", "C7370151-2116-470A-8E26-5F878B3C9D78");
          stationConfig1.put( "C7370151-2116-470A-8E26-5F878B3C9D78","孝感收费站");
    }

    /**
     * 收费站流量统计结果类
     */
    public static class TollStationFlow {
        public String name;          // 收费站名
        public String stake;         // 收费站主线桩号
        public int total;            // 出入口总流量
        public String tYoy;          // 总流量同比增长
        public String tMom;          // 总流量环比增长
        public int entry;            // 入口流量
        public String enYoy;         // 入口流量同比增长
        public String enMom;         // 入口流量环比增长
        public int export;           // 出口流量
        public String exYoy;         // 出口流量同比增长
        public String exMom;         // 出口流量环比增长

        public TollStationFlow(String name, String stake) {
            this.name = name;
            this.stake = stake;
        }

        public JSONObject toJSON() {
            JSONObject json = new JSONObject();
            json.put("name", name);
            json.put("stake", stake);
            json.put("total", total);
            json.put("tYoy", tYoy);
            json.put("tMom", tMom);
            json.put("entry", entry);
            json.put("enYoy", enYoy);
            json.put("enMom", enMom);
            json.put("export", export);
            json.put("exYoy", exYoy);
            json.put("exMom", exMom);
            return json;
        }
    }

    /**
     * 计算收费站流量统计
     * @param stationName 收费站名称
     * @param startTime 开始时间 (格式: yyyy-MM-dd HH:mm:ss)
     * @param endTime 结束时间 (格式: yyyy-MM-dd HH:mm:ss)
     * @return 流量统计结果
     */
    public TollStationFlow calculateFlow(String stationName, String startTime, String endTime) {
        try {
            if (connection == null) {
                connection = ConnectionFactory.createConnection(conf);
            }

            String orgcode = stationConfig.get(stationName);
            if (orgcode == null) {
                throw new IllegalArgumentException("未知的收费站: " + stationName);
            }

            // 解析时间
            LocalDateTime start = LocalDateTime.parse(startTime, TIME_FORMATTER);
            LocalDateTime end = LocalDateTime.parse(endTime, TIME_FORMATTER);

            // 计算当前时间段流量
            FlowData currentFlow = getFlowData(orgcode, start, end);

            // 计算同比流量（去年同期）
            LocalDateTime yoyStart = start.minusYears(1);
            LocalDateTime yoyEnd = end.minusYears(1);
            FlowData yoyFlow = getFlowData(orgcode, yoyStart, yoyEnd);

            // 计算环比流量（上一个周期）
            long hoursBetween = java.time.Duration.between(start, end).toHours();
            LocalDateTime momStart = start.minusHours(hoursBetween);
            LocalDateTime momEnd = end.minusHours(hoursBetween);
            FlowData momFlow = getFlowData(orgcode, momStart, momEnd);

            // 构建结果
            TollStationFlow result = new TollStationFlow(stationName, getStakeNumber(stationName));
            result.entry = currentFlow.entry;
            result.export = currentFlow.export;
            result.total = currentFlow.entry + currentFlow.export;

            // 计算增长率
            result.enYoy = calculateGrowthRate(currentFlow.entry, yoyFlow.entry);
            result.enMom = calculateGrowthRate(currentFlow.entry, momFlow.entry);
            result.exYoy = calculateGrowthRate(currentFlow.export, yoyFlow.export);
            result.exMom = calculateGrowthRate(currentFlow.export, momFlow.export);
            result.tYoy = calculateGrowthRate(result.total, yoyFlow.entry + yoyFlow.export);
            result.tMom = calculateGrowthRate(result.total, momFlow.entry + momFlow.export);

            return result;

        } catch (Exception e) {
            throw new RuntimeException("计算收费站流量失败: " + e.getMessage(), e);
        }
    }


    public seventhData calculateFlowMore(String stationId, String startTime, String endTime) {
        try {
            if (connection == null) {
                connection = ConnectionFactory.createConnection(conf);
            }


            // 解析时间
            LocalDateTime start = LocalDateTime.parse(startTime, TIME_FORMATTER);
            LocalDateTime end = LocalDateTime.parse(endTime, TIME_FORMATTER);

            // 计算当前时间段流量
            FlowDataMore currentFlow = getFlowDataMore(stationId, start, end);
            List<seventhPiece>s=new ArrayList<>();
            s.add(new seventhPiece(1,currentFlow.uptotal,currentFlow.upbus,currentFlow.uptruck, 23.3F));
            s.add(new seventhPiece(2,currentFlow.uptotal,currentFlow.upbus,currentFlow.uptruck, 23.0F));
            seventhData data = new seventhData(stationId, stationConfig1.get(stationId), s);
            return data;


        } catch (Exception e) {
            throw new RuntimeException("计算收费站流量失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取指定时间段的流量数据
     */
    private FlowData getFlowData(String orgcode, LocalDateTime start, LocalDateTime end) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        long startHour = getHourTimestamp(start);
        long endHour = getHourTimestamp(end);

        int entryTotal = 0;
        int exportTotal = 0;

        // 遍历每个小时的时间段
        for (long hour = startHour; hour <= endHour; hour += 3600000) {
            String rowKey = orgcode + "_" + hour;

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (!result.isEmpty()) {
                // 读取上行流量（入口）
                byte[] entryBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upCount"));
                if (entryBytes != null) {
                    entryTotal += Integer.parseInt(Bytes.toString(entryBytes));
                }

                // 读取下行流量（出口）
                byte[] exportBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downCount"));
                if (exportBytes != null) {
                    exportTotal += Integer.parseInt(Bytes.toString(exportBytes));
                }
            }
        }

        table.close();
        return new FlowData(entryTotal, exportTotal);
    }


        private FlowDataMore getFlowDataMore(String orgcode, LocalDateTime start, LocalDateTime end) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        long startHour = getHourTimestamp(start);
        long endHour = getHourTimestamp(end);

        int entryTotal = 0;
        int exportTotal = 0;
        int downbus=0;
        int upbus=0;
        int downtruck=0;
        int uptruck=0;


        // 遍历每个小时的时间段
        for (long hour = startHour; hour <= endHour; hour += 3600000) {
            String rowKey = orgcode + "_" + hour;

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (!result.isEmpty()) {
                // 读取上行流量（入口）
                byte[] entryBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upCount"));
                if (entryBytes != null) {
                    entryTotal += Integer.parseInt(Bytes.toString(entryBytes));
                }

                // 读取下行流量（出口）
                byte[] exportBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downCount"));
                if (exportBytes != null) {
                    exportTotal += Integer.parseInt(Bytes.toString(exportBytes));
                }

                // 读取下行流量（出口）
                byte[] busup = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upBus"));
                if (exportBytes != null) {
                    upbus += Integer.parseInt(Bytes.toString(busup));
                }

                // 读取下行流量（出口）
                byte[] truckup = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upTrack"));
                if (exportBytes != null) {
                    uptruck += Integer.parseInt(Bytes.toString(truckup));
                }

                    // 读取下行流量（出口）
                byte[] busdown = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downBus"));
                if (exportBytes != null) {
                    downbus += Integer.parseInt(Bytes.toString(busdown));
                }

                // 读取下行流量（出口）
                byte[] truckdown = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downTrack"));
                if (exportBytes != null) {
                    downtruck += Integer.parseInt(Bytes.toString(truckdown));
                }


            }
        }

        table.close();
        return new FlowDataMore(entryTotal, upbus,uptruck,exportTotal,downbus,downtruck);
    }
    /**
     * 计算小时级时间戳
     */
    private long getHourTimestamp(LocalDateTime dateTime) {
        LocalDateTime hourStart = dateTime.withMinute(0).withSecond(0).withNano(0);
        return hourStart.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 计算增长率
     */
    private String calculateGrowthRate(int current, int previous) {
        if (previous == 0) {
            return current == 0 ? "0.00%" : "100.00%";
        }

        double growth = ((double) (current - previous) / previous) * 100;
        return String.format("%.2f%%", growth);
    }

    /**
     * 获取收费站桩号（根据实际情况实现）
     */
    private String getStakeNumber(String stationName) {
        // 这里可以根据收费站名称返回对应的桩号
        // 例如从配置文件中读取或硬编码
        switch (stationName) {
            case "孝感收费站":
            case "XG01":
                return "K1122+200";
            default:
                return "未知桩号";
        }
    }

    /**
     * 内部类：流量数据
     */
    private static class FlowData {
        public int entry;    // 入口流量
        public int export;   // 出口流量

        public FlowData(int entry, int export) {
            this.entry = entry;
            this.export = export;
        }
    }

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
  private static class FlowDataMore {
        public int uptotal;
        public int upbus;
        public int uptruck;

        public int downtotal;
        public int downbus;
        public int downtruck;
    }
    /**
     * 批量计算多个收费站的流量
     */
    public Map<String, TollStationFlow> calculateBatchFlow(List<String> stationNames,
                                                          String startTime, String endTime) {
        Map<String, TollStationFlow> results = new HashMap<>();
        for (String stationName : stationNames) {
            try {
                TollStationFlow flow = calculateFlow(stationName, startTime, endTime);
                results.put(stationName, flow);
            } catch (Exception e) {
                System.err.println("计算收费站 " + stationName + " 流量失败: " + e.getMessage());
            }
        }
        return results;
    }

    /**
     * 关闭连接
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 使用示例
    public static void main(String[] args) {
        TollStationFlowCalculator calculator = new TollStationFlowCalculator();

        try {
            // 计算单个收费站流量
            String startTime = "2024-01-01 00:00:00:000";
            String endTime = "2024-01-01 23:59:59:999";

            TollStationFlow flow = calculator.calculateFlow("孝感收费站", startTime, endTime);
            System.out.println(flow.toJSON().toJSONString());

            // 批量计算多个收费站
            List<String> stations = Arrays.asList("孝感收费站");
            Map<String, TollStationFlow> batchResults = calculator.calculateBatchFlow(stations, startTime, endTime);

            for (Map.Entry<String, TollStationFlow> entry : batchResults.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue().toJSON().toJSONString());
            }

        } finally {
            calculator.close();
        }
    }
}