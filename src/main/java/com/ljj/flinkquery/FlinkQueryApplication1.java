package com.ljj.flinkquery;

import com.ljj.flinkquery.demos.entity.data.Utils;
import com.ljj.flinkquery.demos.entity.stat;
import com.ljj.flinkquery.demos.web.impl.myTools;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ljj.flinkquery.demos.entity.data.Utils.PathPoint;

import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.ljj.flinkquery.demos.entity.data.Utils.*;



import static com.ljj.flinkquery.demos.entity.data.Utils.convertToTimestampMillis;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.*;

@SpringBootApplication
public class FlinkQueryApplication1 {

    public static final ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();
    public static RedisTemplate<String, String> redisTemplate;//60s内所有数据
    public static RedisTemplate<String, String> redisTemplate1;//10s内所有数据
    Map<String , Boolean>totalMap = new ConcurrentHashMap<>();
    Map<String , Boolean>tempMap = new ConcurrentHashMap<>();
    static int upcount = 0;
    int downcount = 0;
     public static final AtomicReference<Pair<Integer, Integer>> todayTotal =
        new AtomicReference<>(new Pair<>(0, 0));//数量，处理毫秒数

    public static final AtomicReference<int[]> yearToDateTraffic =
        new AtomicReference<>(new int[]{0, 0});//上行车辆数，下行车辆数

    public static void main(String[] args) throws Exception {
        SpringApplication.run(FlinkQueryApplication.class, args);
         VehicleCounter.scheduleCleanup();
        Pair<Integer, Integer> result = getTodayTotalDataBase(System.currentTimeMillis());
                todayTotal.set(result);
        System.out.println("todayTotal inited: " + todayTotal.get());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int[] result1 = getYearToDateTraffic(System.currentTimeMillis());
                yearToDateTraffic.set(result1);
        env.setParallelism(6);

        // 配置 KafkaSource
        String brokers = "10.48.53.82:9092";
        String groupId = "flink-group-SegCar"; // 消费者组ID

        // 主题列表
        List<String> topics = Arrays.asList(
                "fiberData1",
                "fiberData2",
                "fiberData3",
                "fiberData4",
                "fiberData5",
                "fiberData6",
                "fiberData7",
                "fiberData8",
                "fiberData9",
                "fiberData10",
                "fiberData11");

        // 初始化第一个 KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 创建第一个数据流
        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Sources Save");

 DataStream<PathPoint> flatMapStream = unionStream
    .flatMap(new FlatMapFunction<String, PathPoint>() {
        @Override
        public void flatMap(String jsonString, Collector<PathPoint> out) {
            try {
                JSONObject jsonObject = JSON.parseObject(jsonString);
                for (PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class)) {
                    if (!ppoint.getStakeId().isEmpty()) {
                        Integer vt = ppoint.getVehicleType();
//                        System.out.println("a");
                        ppoint.setOriginalType(vt);
                        ppoint.setTimeStamp(jsonObject.getString("timeStamp"));
//                        System.out.println("b");
                        // 处理新车辆计数
                        VehicleCounter.processVehicle(ppoint);
//                        System.out.println("c");
                        out.collect(ppoint);
                    }
                }
            } catch (Exception e) {
            }
        }
    })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PathPoint>() {
                                                   @Override
                                                   public long extractTimestamp(PathPoint pathPoint, long recordTimestamp) {
                                                       return convertToTimestampMillis(pathPoint.getTimeStamp());
                                                   }
                                               }
                        ).withIdleness(Duration.ofSeconds(30))); // 超过30s不更新则标记为空闲分区;;
//        flatMapStream.print();





        // 按 rowkey 分组并处理
        DataStream<Tuple2<String, String>> processedStream = flatMapStream.keyBy(ppoint -> ppoint.getStakeId().split("\\+")[0])
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2)))
                .aggregate(new AggregateFunction<PathPoint, VehicleSegAccumulator, Tuple2<String, String>>() {
                    @Override
                    public VehicleSegAccumulator createAccumulator() {
                        Map<Long, VehicleSeg> vehicleSegMap = new HashMap<>();
                        return new VehicleSegAccumulator("", vehicleSegMap);
                    }

                    @Override
                    public VehicleSegAccumulator add(PathPoint ppoint, VehicleSegAccumulator vehicleSegAcc) {
                        vehicleSegAcc.setCurrentKey(convertToTimestampMillis(ppoint.getTimeStamp()) / 10000 * 10000 + "_" + ppoint.getStakeId().split("\\+")[0]);
                        Map<Long, VehicleSeg> vehicleSegMap = vehicleSegAcc.getVehicleSegMap();

                        if(!vehicleSegMap.containsKey(ppoint.getId())) {
                            VehicleSeg vehicleSeg = new VehicleSeg(ppoint.getPlateNo(), ppoint.getId(), ppoint.getSpeed(), ppoint.getDirection(), 1, ppoint.getOriginalType(),ppoint.getVehicleType(), ppoint.getSpecialFlag());
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        }
                        else {
                            // 更新vehicleSeg的speedSum和pointSum
                            VehicleSeg vehicleSeg = vehicleSegMap.get(ppoint.getId());
                            vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
                            vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
                            // 显式更新一下
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        }
                        return vehicleSegAcc;
                    }

                    @Override
                    public Tuple2<String, String> getResult(VehicleSegAccumulator vehicleSegAcc) {
                        if (!vehicleSegAcc.getVehicleSegMap().isEmpty()) {
                            List<VehicleSeg> mergedVehicleSeg = new ArrayList<>(vehicleSegAcc.getVehicleSegMap().values());
                            return Tuple2.of(vehicleSegAcc.getCurrentKey(), JSON.toJSONString(mergedVehicleSeg));
                        }
                        // 若出现异常（vehicleSegAcc为空），返回一个空的Tuple2
                        return new Tuple2<>();
                    }

                    @Override
                    public VehicleSegAccumulator merge(VehicleSegAccumulator a, VehicleSegAccumulator b) {
                        return new VehicleSegAccumulator();
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.STRING)); // 显式指定输出类型;

        // 添加内存存储Sink
        processedStream.addSink(new RedisSink());

        // 执行任务
        env.execute("Flink STCar to MemoryMap");

    }







   public static class VehicleCounter {
    // 使用 ConcurrentHashMap 记录车辆最后出现日期
    private static final Map<Long, LocalDate> vehicleLastSeen = new ConcurrentHashMap<>();

    // 每日计数器（日期字符串 -> 计数）
    private static final Map<String, AtomicInteger> dailyCounters = new ConcurrentHashMap<>();

    // 当前日期（根据事件时间）
    private static volatile String currentDate = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

public static void processVehicle(PathPoint point) {
    long vehicleId = point.getId();
    long eventTimeMillis = convertToTimestampMillis(point.getTimeStamp());
    LocalDate eventDate = Instant.ofEpochMilli(eventTimeMillis)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
    String eventDateStr = eventDate.format(DateTimeFormatter.BASIC_ISO_DATE);

    // 处理日期切换
    if (!eventDateStr.equals(currentDate)) {
        currentDate = eventDateStr;
        dailyCounters.put(currentDate, new AtomicInteger(0)); // 新日期计数器
        // 重置今日计数 - 使用Pair的正确访问方法
        Pair<Integer, Integer> current = todayTotal.get();
        todayTotal.set(new Pair<>(0, current.getValue())); // 保留处理时间
    }

    AtomicInteger counter = dailyCounters.get(currentDate);

   if (vehicleLastSeen.getOrDefault(vehicleId, LocalDate.MIN).isBefore(eventDate)) {
    vehicleLastSeen.put(vehicleId, eventDate);
    int newCount = counter.incrementAndGet(); // 获取最新计数

    // 使用最新计数更新todayTotal
    while (true) {
        Pair<Integer, Integer> current = todayTotal.get();
        Pair<Integer, Integer> newPair = new Pair<>(
            newCount,              // 直接使用计数器的最新值
            current.getValue()      // 保留处理时间
        );
        if (todayTotal.compareAndSet(current, newPair)) {
            break;
        }
    }
}
}

    // 添加定时清理任务
    public static void scheduleCleanup() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            LocalDate today = LocalDate.now();
            // 清理30天前的车辆记录
            vehicleLastSeen.entrySet().removeIf(entry ->
                entry.getValue().isBefore(today.minusDays(30))
            );
            // 清理30天前的计数器
            String threshold = today.minusDays(30).format(DateTimeFormatter.BASIC_ISO_DATE);
            dailyCounters.entrySet().removeIf(entry ->
                entry.getKey().compareTo(threshold) < 0
            );
        }, 0, 1, TimeUnit.HOURS); // 每小时清理一次
    }
}
// 生成RowKey方法
    private static String generateRowKey(PathPoint ppoint) {
        long minuteTimestamp = convertToTimestampMillis(ppoint.getTimeStamp()) / 1000 * 1000;
        return minuteTimestamp + "_" + ppoint.getStakeId().split("\\+")[0];
    }
    // 内存存储Sink实现
    static class InMemoryMapSink extends RichSinkFunction<Tuple2<String, String>> {
        @Override
        public void invoke(Tuple2<String, String> value, Context context) {
            // 存储到内存Map
            resultMap.put(value.f0, value.f1);

            // 调试输出（可选）
//            System.out.println("me.remap: " + resultMap);
//            System.out.println("Value Length: " + value.f1.length() + " characters");
        }
    }
static class RedisSink extends RichSinkFunction<Tuple2<String, String>> {
    @Override
    public void open(Configuration parameters) {
        // 从环境变量或配置获取参数
        String redisHost = "100.65.38.141"; // 使用配置中的主机
        int redisPort = 6380;              // 单机端口
        String password = "whdx123cgz666";         // 密码

        // 单机配置
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redisHost);
        config.setPort(redisPort);
        config.setPassword(password);

        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
            .commandTimeout(Duration.ofSeconds(2))
            .build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, clientConfig);
        factory.afterPropertiesSet();

        // 创建Redis模板
        redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(factory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate.afterPropertiesSet();

        // 第二个RedisTemplate
        redisTemplate1 = new RedisTemplate<>();
        redisTemplate1.setConnectionFactory(factory);
        redisTemplate1.setKeySerializer(new StringRedisSerializer());
        redisTemplate1.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate1.afterPropertiesSet();
    }


    @Override
    public void invoke(Tuple2<String, String> value, Context context) {
 // 获取当前 Redis 数据库的键数量
//            Long dbSize = redisTemplate.execute((RedisCallback<Long>) RedisServerCommands::dbSize);
//            // 打印键数量（实际使用时建议使用日志框架）
//            System.out.println("former Redis Key Count: " + dbSize);
        // 使用HBaseServiceImpl中相同的redisTemplate实例
        redisTemplate.opsForValue().set(
            "v60_"+value.f0,
            value.f1,
            60, TimeUnit.SECONDS
        );
         redisTemplate1.opsForValue().set(
                "v2_" + value.f0, // 使用不同前缀
                value.f1,
                2, TimeUnit.SECONDS // 设置10秒过期
            );
         // 获取当前 Redis 数据库的键数量
//            dbSize = redisTemplate.execute((RedisCallback<Long>) RedisServerCommands::dbSize);
//            // 打印键数量（实际使用时建议使用日志框架）
//            System.out.println("Current Redis Key Count: " + dbSize);
    }
}

    // 车辆聚合处理器（保持不变）
    private static class VehicleAggregator extends KeyedProcessFunction<String, PathPoint, Tuple2<String, String>> {
        private transient MapState<Long, VehicleSeg> vehicleSegState;
        private transient ValueState<Boolean> timerState;

        private final StateTtlConfig vehiclettlConfig = StateTtlConfig
                .newBuilder(Time.seconds(120))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Long, VehicleSeg> vehicleSegDescriptor =
                    new MapStateDescriptor<>("vehicleState", Types.LONG, TypeInformation.of(VehicleSeg.class));
            vehicleSegDescriptor.enableTimeToLive(vehiclettlConfig);
            vehicleSegState = getRuntimeContext().getMapState(vehicleSegDescriptor);

            ValueStateDescriptor<Boolean> timerDesc =
                    new ValueStateDescriptor<>("timer-state", Boolean.class);
            timerState = getRuntimeContext().getState(timerDesc);
        }

        @Override
        public void processElement(PathPoint ppoint, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if (!vehicleSegState.contains(ppoint.getId())) {
                VehicleSeg vehicleSeg = new VehicleSeg(
                        ppoint.getPlateNo(),
                        ppoint.getId(),
                        ppoint.getSpeed(),
                        ppoint.getDirection(),
                        1,
                        ppoint.getOriginalType(),
                        ppoint.getVehicleType(),
                        ppoint.getSpecialFlag()
                );
                vehicleSegState.put(ppoint.getId(), vehicleSeg);
            } else {
                VehicleSeg vehicleSeg = vehicleSegState.get(ppoint.getId());
                vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
                vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
            }

            if (timerState.value() == null || !timerState.value()) {
                long timerInterval = 1000;
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + timerInterval
                );
                timerState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if (!vehicleSegState.isEmpty()) {
                List<VehicleSeg> merged = new ArrayList<>();
                for (VehicleSeg seg : vehicleSegState.values()) {
                    merged.add(seg);
                }
                out.collect(new Tuple2<>(ctx.getCurrentKey(), JSON.toJSONString(merged)));
            }
            timerState.clear();
            vehicleSegState.clear();
        }
    }

    // 车辆分段数据实体类
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleSeg {
        private String plateNo;
        private long carId;
        private float speedSum;
        private int direction;
        private int pointSum;
        private Integer originalType = null;
    private Integer vehicleType=null;
        private String specialFlag = null;
    }


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public static class VehicleSegAccumulator {
    private String currentKey;  // 存储键值
    private Map<Long, VehicleSeg> vehicleSegMap;
}

//查询指定时间戳当天的车流量
public static Pair<Integer, Integer> getTodayTotal(long timeMillis) throws IOException {
    long st = System.currentTimeMillis();

    // 1. 确定目标日期
    LocalDate targetDate = Instant.ofEpochMilli(timeMillis)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
    String dateStr = targetDate.format(DateTimeFormatter.BASIC_ISO_DATE);

    // 2. 首先尝试从Redis获取实时数据
    String redisValue = redisTemplate.opsForValue().get("traffic_daily:" + dateStr);
    if (redisValue != null) {
        int count = Integer.parseInt(redisValue);
        return new Pair<>(count, 0);
    }

    // 3. Redis中没有数据时从HBase获取
    org.apache.hadoop.conf.Configuration conf = getHBaseConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
        String tableName = "ZCarTraj_" + dateStr;

        if (!tableExists(connection, tableName)) {
            System.out.println("跳过不存在的表: " + tableName);
            return new Pair<>(0, 0);
        }

        long[] dateRange = getDateRange(targetDate);
        System.out.println("处理日期: " + dateStr + ", 时间范围: " + dateRange[0] + " - " + dateRange[1]);

        int count = 0;
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {

            for (Result res : scanner) {
                count++;
            }
        }

        long st1 = System.currentTimeMillis();
        return new Pair<>(count, (int)(st1 - st));
    }
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
    org.apache.hadoop.conf.Configuration conf = getHBaseConfiguration();

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

        // 4. 如果没有需要扫描的表，直接返回
        if (tablesToScan.isEmpty()) {
            return new int[]{0, 0};
        }

        // 5. 创建线程池并行处理表扫描
        int threadCount = Math.max(1, Math.min(tablesToScan.size(), 10)); // 确保至少1个线程
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
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

        // 6. 汇总结果
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
public static Pair<Integer, Integer> getTodayTotalMemory(long timeMillis) throws IOException {
        return todayTotal.get();
}
}