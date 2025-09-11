package com.ljj.flinkquery.demos.web.impl.edu.querys;

import com.ljj.flinkquery.demos.entity.watch.zaEachSitData;
import com.ljj.flinkquery.demos.entity.watch.zaEachSitPiece;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RampTrafficQuery {

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80";
    private static final String HBASE_ZOOKEEPER_PORT = "2181";
    private static final String TABLE_NAME = "ramp_hourly_traffic";
    private static final String COLUMN_FAMILY = "cf";

    // 时间格式化
    public static final DateTimeFormatter INPUT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHH");
    private static final DateTimeFormatter OUTPUT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");


    /**
     * 查询匝道交通数据
     */
    public static List<zaEachSitData> queryRampTrafficData(LocalDateTime startTime, LocalDateTime endTime)
            throws Exception {
 Map<String, String> idToName = new HashMap<>();
        idToName.put("94EFD350-B555-4A6A-9A47-344174C85B5D", "孝感互通");
        idToName.put("key2", "value2");
        idToName.put("key3", "value3");
        List<zaEachSitData> result = new ArrayList<>();

        // 创建HBase配置和连接
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT);

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            // 计算时间范围内的小时数
            long hours = ChronoUnit.HOURS.between(startTime, endTime) + 1;

            // 遍历每个小时
            for (int i = 0; i < hours; i++) {
                LocalDateTime currentHour = startTime.plusHours(i);
                String hourKey = currentHour.format(ROWKEY_FORMATTER);

                // 创建每小时的数据对象
                zaEachSitData hourlyData = new zaEachSitData();
                hourlyData.setTime(currentHour.format(OUTPUT_FORMATTER));
                hourlyData.setDirStaList(new ArrayList<>());

                // 查询四个匝道(A,B,C,D)的数据
                for (char ramp : new char[]{'A', 'B', 'C', 'D'}) {
                    String rowKey = hourKey + "_" + ramp;

                    // 创建Get对象
                    Get get = new Get(Bytes.toBytes(rowKey));

                    // 执行查询
                    Result hbaseResult = table.get(get);

                    if (!hbaseResult.isEmpty()) {
                        // 解析结果
                        zaEachSitPiece rampData = new zaEachSitPiece();
                        rampData.setDirection(getDirectionFromRamp(ramp));

                        // 获取各列的值
                        byte[] totalBytes = hbaseResult.getValue(
                                Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_vehicles"));
                        byte[] busBytes = hbaseResult.getValue(
                                Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"));
                        byte[] truckBytes = hbaseResult.getValue(
                                Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("truck_count"));
                        byte[] speedBytes = hbaseResult.getValue(
                                Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_speed"));

                        if (totalBytes != null) {
                            rampData.setTotal(Integer.parseInt(Bytes.toString(totalBytes)));
                        }
                        if (busBytes != null) {
                            rampData.setMinibus(Integer.parseInt(Bytes.toString(busBytes)));
                        }
                        if (truckBytes != null) {
                            rampData.setTruck(Integer.parseInt(Bytes.toString(truckBytes)));
                        }
                        if (speedBytes != null) {
                            rampData.setSpeed(Double.parseDouble(Bytes.toString(speedBytes)));
                        }

                        hourlyData.getDirStaList().add(rampData);
                    } else {
                        // 如果没有数据，添加默认值
                        zaEachSitPiece rampData = new zaEachSitPiece();
                        rampData.setDirection(getDirectionFromRamp(ramp));
                        rampData.setTotal(0);
                        rampData.setMinibus(0);
                        rampData.setTruck(0);
                        rampData.setSpeed(0.0);
                        hourlyData.getDirStaList().add(rampData);
                    }
                }
                hourlyData.setStationId(idToName.get(hourKey));
                result.add(hourlyData);
            }
        }

        return result;
    }

    /**
     * 将匝道字母转换为方向数字
     */
    private static int getDirectionFromRamp(char ramp) {
        switch (ramp) {
            case 'A': return 1;
            case 'B': return 2;
            case 'C': return 3;
            case 'D': return 4;
            default: return 0;
        }
    }





}