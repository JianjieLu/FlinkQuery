//package com.ljj.flinkquery.demos.web.service;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import com.ljj.flinkquery.demos.entity.data.Utils.PathPoint;
//import lombok.*;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.*;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.util.Collector;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.locks.ReentrantLock;
//
//import static com.ljj.flinkquery.demos.entity.data.Utils.convertToTimestampMillis;
//
///**
// * 更新VehicleSeg的字段
// */
//public class SegCarIngestMoniOfiV5 {
//public static final ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();
//    public static void main(String[] args) throws Exception {
//        // 设置 Flink 流执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//
//        // 配置 KafkaSource
//        String brokers = "100.65.38.40:9092";
//        String groupId = "flink-group"; // 消费者组ID
//
//        // 主题列表
//        List<String> topics = Arrays.asList("MergedPathData",
//                "MergedPathData.sceneTest.1",
//                "MergedPathData.sceneTest.2",
//                "MergedPathData.sceneTest.3",
//                "MergedPathData.sceneTest.4",
//                "MergedPathData.sceneTest.5");
//
//        // 初始化第一个 KafkaSource
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers(brokers)
//                .setTopics(topics.get(0))
//                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setProperty("auto.offset.commit", "true")
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        // 创建第一个数据流
//        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");
//
//        // 循环添加其他主题的数据流并合并
//        for (int i = 1; i < topics.size(); i++) {
//            KafkaSource<String> source = KafkaSource.<String>builder()
//                    .setBootstrapServers(brokers)
//                    .setTopics(topics.get(i))
//                    .setGroupId(groupId)
//                    .setStartingOffsets(OffsetsInitializer.latest())
//                    .setProperty("auto.offset.commit", "true")
//                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .build();
//
//            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
//            unionStream = unionStream.union(stream);
//        }
//
//        // 保存 flatMap 操作后的结果
//        DataStream<PathPoint> flatMapStream = unionStream
//                .flatMap(new FlatMapFunction<String, PathPoint>() {
//                    @Override
//                    public void flatMap(String jsonString, Collector<PathPoint> out) {
//                        try {
//                            JSONObject jsonObject = JSON.parseObject(jsonString);
//                            String timeStampStr = jsonObject.getString("timeStamp");
//                            long timeObs;
//                            try {
//                                // 尝试按三位毫秒格式解析
//                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
//                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
//                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                            } catch (Exception e) {
//                                // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
//                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
//                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
//                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                            }
//
//                            // 截取到分钟级的时间戳
//                            long minuteTimestamp = timeObs / 60000 * 60000;
//
//                            for(PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class))
//                                if(!ppoint.getStakeId().equals("") && !"ABCD".contains(Character.toString(ppoint.getStakeId().charAt(0))))
//                                    out.collect(ppoint);
//
//                        } catch (Exception e) {
//                            System.err.println("解析 JSON 时出错: " + e.getMessage());
//                        }
//                    }
//                });
//
////        flatMapStream.print();
//
//        // 按 rowkey 分组并处理
//        DataStream<Tuple2<String, String>> processedStream = flatMapStream.keyBy(ppoint -> (convertToTimestampMillis(ppoint.getTimeStamp()) / 60000 * 60000) + "_" + ppoint.getStakeId().split("\\+")[0])
//                .process(new KeyedProcessFunction<String, PathPoint, Tuple2<String, String>>() {
//                    private transient MapState<Long, VehicleSeg> vehicleSegState;
//                    private transient ValueState<Boolean> timerState;
//                    private final long timerInterval = 60 * 1000; // 60 秒
////                    private boolean timerRegistered = false;
//
//                    private final StateTtlConfig vehiclettlConfig = StateTtlConfig
//                            .newBuilder(Time.seconds(120))
//                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 每次写入时更新存活时间，每辆车一开始写入到最后写入是60s，实际会缓存120s
//                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
//                            .build();
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        MapStateDescriptor<Long, VehicleSeg> vehicleSegDescriptor =
//                                new MapStateDescriptor<>("vehicleState", Types.LONG, TypeInformation.of(VehicleSeg.class));
//                        vehicleSegDescriptor.enableTimeToLive(vehiclettlConfig);
//                        vehicleSegState = getRuntimeContext().getMapState(vehicleSegDescriptor);
//
//                        ValueStateDescriptor<Boolean> timerDesc =
//                                new ValueStateDescriptor<>("timer-state", Boolean.class);
//                        timerState = getRuntimeContext().getState(timerDesc);
////                        vehicleSegState = new HashMap<>();
//                    }
//
//                    @Override
//                    public void processElement(PathPoint ppoint, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
//                        if(!vehicleSegState.contains(ppoint.getId())) {
//                            // 初始化vehicleSeg
//                            VehicleSeg vehicleSeg = new VehicleSeg(ppoint.getPlateNo(), ppoint.getId(), ppoint.getSpeed(), ppoint.getDirection(), 1, ppoint.getOriginalType(), ppoint.getSpecialFlag());
//                            vehicleSegState.put(ppoint.getId(), vehicleSeg);
//                        }
//                        else {
//                            // 更新vehicleSeg的speedSum和pointSum
//                            VehicleSeg vehicleSeg = vehicleSegState.get(ppoint.getId());
//                            vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
//                            vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
//                        }
//                        if (timerState.value() == null || !timerState.value()) {
//                            long currentTime = ctx.timerService().currentProcessingTime();
//                            long triggerTime = currentTime + timerInterval;
//                            ctx.timerService().registerProcessingTimeTimer(triggerTime);
//                            timerState.update(true);
//                        }
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
//                        if (!vehicleSegState.isEmpty()) {
//                            List<VehicleSeg> mergedVehicleSeg = new ArrayList<>();
//                            for(VehicleSeg vehicleSeg : vehicleSegState.values())
//                                mergedVehicleSeg.add(vehicleSeg);
//                            out.collect(new Tuple2<>(ctx.getCurrentKey(), JSON.toJSONString(mergedVehicleSeg)));
//                        }
////                        timerRegistered = false;
//                        timerState.clear();
//                        vehicleSegState.clear();
//                    }
//                }) .returns(Types.TUPLE(Types.STRING, Types.STRING)); // 显式指定输出类型;
//
//
//        // 添加 Sink（终端操作）
////        processedStream.addSink(new DynamicHBaseSink("STCar", "cf"));
//
//processedStream.addSink(new InMemoryMapSink());
////        System.out.println(resultMap);
//        env.execute("Flink STCar to HBase");
//    }
//
////     HBase Sink 实现
//    public static class DynamicHBaseSink extends RichSinkFunction<Tuple2<String, String>> {
//        private final String baseTableName;
//        private final String columnFamily;
//
//        private transient org.apache.hadoop.conf.Configuration hadoopConf;
//        private transient Connection hbaseConnection;
//        private transient Table hbaseTable;
//
//        private transient String currentTableName;
//        private transient Long nextTableSwitchTime;
//        private final ReentrantLock tableLock = new ReentrantLock();
//        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
//
//        public DynamicHBaseSink(String baseTableName, String columnFamily) {
//            this.baseTableName = baseTableName;
//            this.columnFamily = columnFamily;
//        }
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            hadoopConf = HBaseConfiguration.create();
//            hadoopConf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141");
//            hadoopConf.set("hbase.zookeeper.property.clientPort", "2181");
//            hadoopConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "400");
//            hadoopConf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
//            hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//            hbaseConnection = ConnectionFactory.createConnection(hadoopConf);
//
//            currentTableName = null;
//            nextTableSwitchTime = null;
//        }
//
//        @Override
//        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
//            tableLock.lock();
//            try {
//                long rowKeyTime = Long.parseLong(value.f0.split("_")[0]);
////                System.out.println("Sink Invoke - RowKey: " + value.f0 + ", Car Numbers: " + value.f1);
//
//                if (currentTableName == null || isTimeToSwitch(rowKeyTime)) {
//                    switchTable(rowKeyTime);
//                }
//
//                String rowKey = value.f0;
//                Put put = new Put(Bytes.toBytes(rowKey));
//                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("VehicleSegments"), Bytes.toBytes(value.f1.toString()));
//                hbaseTable.put(put);
//            } finally {
//                tableLock.unlock();
//            }
//        }
//
//        @Override
//        public void close() throws Exception {
//            if (hbaseTable != null) {
//                hbaseTable.close();
//            }
//            if (hbaseConnection != null) {
//                hbaseConnection.close();
//            }
//        }
//
//        private boolean isTimeToSwitch(long rowKeyTime) {
//            // 检测是否为下一小时
//            return rowKeyTime >= nextTableSwitchTime;
//        }
//
//        private void switchTable(long rowKeyTime) throws Exception {
//            tableLock.lock();
//            try {
//                if (nextTableSwitchTime == null) {
//                    nextTableSwitchTime = (rowKeyTime / 1000 / 3600 * 3600 + 3600) * 1000;
//                }
//
//                String timestamp = convertToHBaseTableTime(rowKeyTime);
//                currentTableName = baseTableName + "_" + timestamp;
//
//                createTableIfNotExists(currentTableName, columnFamily);
//
//                if (hbaseTable != null) {
//                    hbaseTable.close();
//                }
//                hbaseTable = hbaseConnection.getTable(TableName.valueOf(currentTableName));
//
//                nextTableSwitchTime = (rowKeyTime / 1000 / 3600 * 3600 + 3600) * 1000;
//
//                System.out.printf("切换到新表: %s，下一次切换时间: %s%n", currentTableName, convertToHBaseTableTime(nextTableSwitchTime));
//            } finally {
//                tableLock.unlock();
//            }
//        }
//
//        private void createTableIfNotExists(String tableName, String columnFamily) {
//            tableLock.lock();
//            try (Admin admin = hbaseConnection.getAdmin()) {
//                TableName hbaseTableName = TableName.valueOf(tableName);
//
//                Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
//
//                synchronized (lock) {
//                    // 获取最新的表列表
//                    admin.listTables();
//                    if (!admin.tableExists(hbaseTableName)) {
//                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
//                        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
//                        try {
//                            admin.createTable(tableDescriptor);
//                            System.out.println("Table created: " + tableName);
//                        } catch (TableExistsException e) {
//                            System.out.println("Table already exists, but not detected by tableExists(): " + tableName);
//                        }
//                    } else {
//                        System.out.println("Table already exists: " + tableName);
//                    }
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                tableLock.unlock();
//            }
//        }
//    }
//
//    /**
//     *  VehicleSeg 表示一辆车某分钟内监测的信息
//     */
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    @Getter
//    @Setter
//    public static class VehicleSeg {
//        private String plateNo;
//        private long carId;
//        private float speedSum;
//        private int direction;
//        private int pointSum;
//        private Integer originalType = null;
//        private String specialFlag = null;
//    }
//static class InMemoryMapSink extends RichSinkFunction<Tuple2<String, String>> {
//    @Override
//    public void invoke(Tuple2<String, String> value, Context context) {
//        resultMap.put(value.f0, value.f1);
//    }
//}
//    public static String convertToHBaseTableTime(long timestamp) {
//        // 定义日期时间格式
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HH");
//
//        // 将时间戳转换为 Instant 对象
//        Instant instant = Instant.ofEpochMilli(timestamp);
//
//        // 将 Instant 转换为 LocalDateTime（考虑系统默认时区）
//        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
//
//        // 格式化为字符串
//        String dateTimeStr = dateTime.format(formatter);
//
//        return dateTimeStr;
//    }
//}