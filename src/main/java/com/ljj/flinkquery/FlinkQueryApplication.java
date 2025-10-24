package com.ljj.flinkquery;

import com.alibaba.fastjson2.JSON;
import com.ljj.flinkquery.demos.entity.data.Utils.PathPoint;
import javafx.util.Pair;
import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
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
import java.util.concurrent.locks.ReentrantLock;

import static com.ljj.flinkquery.demos.entity.data.Utils.convertToTimestampMillis;
import static com.ljj.flinkquery.demos.entity.data.Utils.eventInfo;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getHBaseConfiguration;
import static com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps.getTodayTotalDataBase;
import static com.ljj.flinkquery.demos.web.service.HBaseServiceImpl.cnum;
//10.3  增加log，redis、todaytotal每天清空。log修改后已经重新打包好，还没上传。

@SpringBootApplication
public class FlinkQueryApplication {
        private static final Logger logger = LoggerFactory.getLogger("whu.edu.moniData.CarTrajIngestMoniOfi");

        private static final Map<String, String> mapPlateNo = new ConcurrentHashMap<>(); // 新增：存储车牌号

//    private static final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
    private static final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
    private static final Map<String, Integer> mapType = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastSeenTime = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastSampleTime = new ConcurrentHashMap<>();
    private static final ReentrantLock stateLock = new ReentrantLock();
 // 年度累计流量计数器（上行/下行）
        private static final AtomicInteger upTotal = new AtomicInteger(0);
        private static final AtomicInteger downTotal = new AtomicInteger(0);
    // 新增：存储车辆在计算范围内的时间二元组
    private static final Map<String, Map<String, TimePair>> vehicleTimePairs = new ConcurrentHashMap<>();
    // 新增：存储每个分组的时间二元组列表
    private static final Map<String, List<TimePair>> groupedTimePairs = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();
    public static RedisTemplate<String, String> redisTemplate;//60s内所有数据
    public static RedisTemplate<String, String> redisTemplate1;//10s内所有数据
    Map<String, Boolean> totalMap = new ConcurrentHashMap<>();
    Map<String, Boolean> tempMap = new ConcurrentHashMap<>();
    static int upcount = 0;
    int downcount = 0;

    public static final AtomicReference<Pair<Integer, Long>> todayTotal =
            new AtomicReference<>(new Pair<>(0, 0L));//数量，处理毫秒数

    public static final AtomicReference<int[]> yearToDateTraffic =
            new AtomicReference<>(new int[]{0, 0});//上行车辆数，下行车辆数
// 添加清理线程池
    private static final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(1);

    // 静态初始化块 - 在这里放置清理代码
    static {
        cleaner.scheduleAtFixedRate(() -> {
            stateLock.lock();
            try {
                long now = System.currentTimeMillis();
                logger.debug("开始清理过期数据，当前时间: {}", now);

                // 清理30分钟未更新的车辆
                int removedCount = 0;
                Iterator<Map.Entry<String, Long>> iterator = lastSeenTime.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Long> entry = iterator.next();
                    if ((now - entry.getValue()) > 30 * 60 * 1000) {
                        String vehicleId = entry.getKey();
                        iterator.remove();
                        mapPlateNo.remove(vehicleId);
                        mapTimeSeg.remove(vehicleId);
                        mapType.remove(vehicleId);
                        lastSampleTime.remove(vehicleId);
                        vehicleTimePairs.remove(vehicleId);
                        removedCount++;
                    }
                }

                // 清理groupedTimePairs中过期的分组
                groupedTimePairs.entrySet().removeIf(entry ->
                    entry.getValue().removeIf(pair ->
                        (now - pair.lastTime) > 30 * 60 * 1000
                    ) && entry.getValue().isEmpty()
                );

                logger.debug("清理完成，移除了 {} 个过期车辆", removedCount);
                logger.debug("当前集合大小 - lastSeenTime: {}, mapPlateNo: {}, vehicleTimePairs: {}, groupedTimePairs: {}",
                    lastSeenTime.size(), mapPlateNo.size(), vehicleTimePairs.size(), groupedTimePairs.size());

            } catch (Exception e) {
                logger.error("清理过程中发生异常", e);
            } finally {
                stateLock.unlock();
            }
        }, 10, 10, TimeUnit.MINUTES); // 延迟10分钟启动，然后每10分钟执行一次
    }
  // 添加关闭钩子，确保清理线程池被正确关闭
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("应用关闭，正在停止清理线程...");
            cleaner.shutdown();
            try {
                if (!cleaner.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleaner.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleaner.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("清理线程已停止");
        }));
    }
    public static void main(String[] args) throws Exception {
         logger.debug("测试日志输出"); // DEBUG级别日志
        SpringApplication.run(FlinkQueryApplication.class, args);
        VehicleCounter.scheduleCleanup();
        Pair<Integer, Long> result = getTodayTotalDataBase(System.currentTimeMillis());
        todayTotal.set(result);
        System.out.println("todayTotal inited: " + todayTotal.get());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int[] result1 = getYearToDateTraffic(System.currentTimeMillis());
        upTotal.set(upTotal.get() + result1[0]);
        downTotal.set(downTotal.get() + result1[1]);
        yearToDateTraffic.set(result1);

        env.setParallelism(6);
   // 解析命令行参数
    String brokers = "10.48.53.82:9092"; // 默认值
    List<String> topics = new ArrayList<>();

    for (int i = 0; i < args.length; i++) {
        if ("--brokers".equals(args[i]) && i + 1 < args.length) {
            brokers = args[++i];
        } else if ("--topics".equals(args[i]) && i + 1 < args.length) {
            // 支持逗号分隔的多个topic
            Collections.addAll(topics, args[++i].split(","));
        }
    }

    // 如果没有指定topics，使用默认值
    if (topics.isEmpty()) {
        topics = Arrays.asList("jtkj.jga.path"); // 默认topic
    }

    System.out.println("Using brokers: " + brokers);
    System.out.println("Using topics: " + topics);
//
        // 初始化第一个 KafkaSource
    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(brokers)
            .setTopics(topics)
            .setGroupId("flink-group-SegCar1")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setProperty("auto.offset.commit", "true")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        // 创建第一个数据流
        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Sources Save");

//        KafkaSink<String> primarySink = KafkaSink.<String>builder()
//                .setBootstrapServers(brokers)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("trajectoryoutput")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .build();
//        SingleOutputStreamOperator<String> primaryProcessed = unionStream
//                .flatMap(new PrimaryTrajectoryProcessor())
//                .name("Primary Trajectory Processor");

        DataStream<PathPoint> flatMapStream = unionStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String jsonString, Collector<PathPoint> out) {
                        try {
                            com.alibaba.fastjson2.JSONObject jsonObject = JSON.parseObject(jsonString);
                            for (PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class)) {
                                if (!ppoint.getStakeId().isEmpty()) {
                                    Integer vt = ppoint.getVehicleType();
                                    ppoint.setTimeStamp(jsonObject.getString("timeStamp"));

                                    // 新增：处理车辆在计算范围内的时间
                                    processVehicleInRange(ppoint);

                                    // 处理新车辆计数
                                    VehicleCounter.processVehicle(ppoint);
                                    long eventTime = convertToTimestampMillis(ppoint.getTimeStamp());
                                    int direction = ppoint.getDirection();
                                    int[] current;
                                    int[] next = new int[2];
                                    do {
                                        current = yearToDateTraffic.get();
                                        next[0] = current[0] + (direction == 1 ? 1 : 0);
                                        next[1] = current[1] + (direction == 2 ? 1 : 0);
                                    } while (!yearToDateTraffic.compareAndSet(current, next));

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
                        vehicleSegAcc.setCurrentKey(convertToTimestampMillis(ppoint.getTimeStamp()) / 1000 * 1000 + "_" + ppoint.getStakeId().split("\\+")[0]);
                        Map<Long, VehicleSeg> vehicleSegMap = vehicleSegAcc.getVehicleSegMap();

                        if (!vehicleSegMap.containsKey(ppoint.getId())) {
                            VehicleSeg vehicleSeg = new VehicleSeg(ppoint.getPlateNo(), ppoint.getId(), ppoint.getSpeed(), ppoint.getDirection(), 1, ppoint.getOriginalType(), ppoint.getVehicleType(), ppoint.getSpecialFlag());
                            vehicleSegMap.put(ppoint.getId(), vehicleSeg);
                        } else {
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
//        primaryProcessed.sinkTo(primarySink).name("Primary Output Sink");

        // 执行任务
        env.execute("Flink STCar to MemoryMap");

    }

    // 新增：处理车辆在计算范围内的时间
    private static void processVehicleInRange(PathPoint ppoint) {
        try {
            // 解析桩号
            String stakeId = ppoint.getStakeId();
            long stakeMeters = parseStakeId(stakeId);

            // 检查是否在计算范围内
            if (isNearThousand((int) stakeMeters)) {
                String vehicleId = String.valueOf(ppoint.getId());
                long timestamp = convertToTimestampMillis(ppoint.getTimeStamp());

                // 获取车道、方向、桩号和时间段（分钟）
                int laneNo = ppoint.getLaneNo();
                int direction = ppoint.getDirection();
                String minute = getMinuteFromTimestamp(ppoint.getTimeStamp());

                // 创建分组键
                String groupKey = String.format("%d_%d_%d_%s", laneNo, direction, (stakeMeters / 1000) * 1000, minute);

                // 获取或创建车辆的时间二元组
                Map<String, TimePair> vehicleMap = vehicleTimePairs.computeIfAbsent(vehicleId, k -> new ConcurrentHashMap<>());
                TimePair timePair = vehicleMap.computeIfAbsent(groupKey, k -> new TimePair());

                // 更新时间二元组
                if (timePair.firstTime == 0) {
                    timePair.firstTime = timestamp;
                }
                timePair.lastTime = timestamp;

                // 更新分组的时间二元组列表
                List<TimePair> groupList = groupedTimePairs.computeIfAbsent(groupKey, k -> new CopyOnWriteArrayList<>());
                groupList.add(timePair);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 新增：解析桩号为米
    private static long parseStakeId(String stakeId) {
        try {
            String[] parts = stakeId.split("\\+");
            long km = Long.parseLong(parts[0]);
            long m = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
            return km * 1000 + m;
        } catch (Exception e) {
            return -1; // 无效桩号
        }
    }

    // 新增：判断是否在计算范围内
    public static boolean isNearThousand(int n) {
        // 计算最近的整一千数（考虑四舍五入）
        long roundedThousand;
        if (n >= 0) {
            roundedThousand = Math.round(n / 1000.0) * 1000;
        } else {
            // 对于负数，需要特殊处理四舍五入
            roundedThousand = Math.round(n / 1000.0) * 1000;
        }

        // 计算绝对差值
        long diff = Math.abs(n - roundedThousand);

        // 判断差值是否在12以内
        return diff <= 12;
    }

    // 新增：从时间戳获取分钟字符串
    private static String getMinuteFromTimestamp(String timestamp) {
        try {
            long millis = convertToTimestampMillis(timestamp);
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
            return dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        } catch (Exception e) {
            return "unknown";
        }
    }

    // 新增：时间二元组类
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TimePair {
        private long firstTime;
        private long lastTime;
    }

    // 新增：获取分组的时间二元组列表
    public static List<TimePair> getGroupTimePairs(String groupKey) {
        return groupedTimePairs.getOrDefault(groupKey, Collections.emptyList());
    }

    // 新增：获取车辆的时间二元组
    public static Map<String, TimePair> getVehicleTimePairs(String vehicleId) {
        return vehicleTimePairs.getOrDefault(vehicleId, Collections.emptyMap());
    }

    public static class VehicleCounter {
        // 使用 ConcurrentHashMap 记录车辆最后出现日期
        private static final Map<Long, LocalDate> vehicleLastSeen = new ConcurrentHashMap<>();
        // 每日计数器（日期字符串 -> 计数）
        private static final Map<String, AtomicInteger> dailyCounters = new ConcurrentHashMap<>();
        // 当前日期（根据事件时间）
        private static volatile String currentDate = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);


        // 车辆方向缓存（防止重复计数）
        private static final ConcurrentHashMap<Long, Integer> directionCache = new ConcurrentHashMap<>();
        // 方向缓存清理阈值（30天）
        private static final int CACHE_EXPIRE_DAYS = 30;

        public static void processVehicle(PathPoint point) {


            long vehicleId = point.getId();
            int direction = point.getDirection();
            long eventTimeMillis = convertToTimestampMillis(point.getTimeStamp());
            LocalDate eventDate = Instant.ofEpochMilli(eventTimeMillis)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            String eventDateStr = eventDate.format(DateTimeFormatter.BASIC_ISO_DATE);

            // 处理日期切换 - 修复时间戳
            if (!eventDateStr.equals(currentDate)) {
                currentDate = eventDateStr;
                dailyCounters.put(currentDate, new AtomicInteger(0));
                // 使用当前时间戳，而不是旧值
                todayTotal.set(new Pair<>(0, System.currentTimeMillis()));
            }

            // 更新当日计数器
            AtomicInteger counter = dailyCounters.computeIfAbsent(currentDate, k -> new AtomicInteger(0));

            if (vehicleLastSeen.getOrDefault(vehicleId, LocalDate.MIN).isBefore(eventDate)) {
                vehicleLastSeen.put(vehicleId, eventDate);
                int newCount = counter.incrementAndGet();

                // 修复更新逻辑 - 使用当前时间戳
                while (true) {
                    Pair<Integer, Long> current = todayTotal.get();
                    if (todayTotal.compareAndSet(current, new Pair<>(
                            newCount,
                            System.currentTimeMillis()  // 使用当前时间戳
                    ))) {
                        break;
                    }
                }
            }
        }

        private static final ScheduledExecutorService dateChecker = Executors.newSingleThreadScheduledExecutor();

        static {
            // 每分钟检查日期切换
            dateChecker.scheduleAtFixedRate(() -> {
                String today = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
                if (!today.equals(currentDate)) {
                    synchronized (VehicleCounter.class) {
                        if (!today.equals(currentDate)) {
                            currentDate = today;
                            dailyCounters.put(currentDate, new AtomicInteger(0));
                            todayTotal.set(new Pair<>(0, todayTotal.get().getValue()));
                        }
                    }
                }
            }, 0, 1, TimeUnit.MINUTES);
        }

        // 添加定时清理任务
        public static void scheduleCleanup() {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                LocalDate today = LocalDate.now();
                LocalDate threshold = today.minusDays(CACHE_EXPIRE_DAYS);

                // 清理车辆最后出现记录
                vehicleLastSeen.entrySet().removeIf(entry ->
                        entry.getValue().isBefore(threshold)
                );

                // 清理每日计数器
                String dateThreshold = threshold.format(DateTimeFormatter.BASIC_ISO_DATE);
                dailyCounters.entrySet().removeIf(entry ->
                        entry.getKey().compareTo(dateThreshold) < 0
                );

                // 清理方向缓存
                directionCache.entrySet().removeIf(entry -> {
                    LocalDate lastSeen = vehicleLastSeen.get(entry.getKey());
                    return lastSeen == null || lastSeen.isBefore(threshold);
                });
            }, 0, 1, TimeUnit.HOURS);
        }

        // 获取年度累计流量
        public static int[] getYearToDateTrafficMemory() {
            return new int[]{upTotal.get(), downTotal.get()};
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
                    .commandTimeout(Duration.ofSeconds(6))
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
                    "v60_" + value.f0,
                    value.f1,
                    60, TimeUnit.SECONDS
            );
            redisTemplate1.opsForValue().set(
                    "v2_" + value.f0, // 使用不同前缀
                    value.f1,
                    10, TimeUnit.SECONDS // 设置10秒过期
            );
//            System.out.println("v60_" + value.f0+":  "+redisTemplate1.opsForValue().get("v60_" + value.f0));
//            System.out.println("v2_" + value.f0+":  "+redisTemplate1.opsForValue().get("v2_" + value.f0));
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
        private Integer vehicleType = null;
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


    public static int[] getYearToDateTraffic(long timestamp) throws IOException {
        // 1. 确定时间范围
//        ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
//        int year = dateTime.getYear();
//        long startOfYear = LocalDate.of(year, 1, 1)
//                .atStartOfDay(ZoneId.systemDefault())
//                .toInstant()
//                .toEpochMilli();
//        long endOfQuery = timestamp + 1; // 查询结束时间（开区间）
//
//        // 2. 预聚合数据结构
//        int upTotal = 0;
//        int downTotal = 0;
//        org.apache.hadoop.conf.Configuration conf = getHBaseConfiguration();
//
//        try (Connection connection = ConnectionFactory.createConnection(conf)) {
//            // 3. 获取所有需要查询的表
//            LocalDate startDate = LocalDate.of(year, 1, 1);
//            LocalDate endDate = dateTime.toLocalDate();
//            List<TableName> tablesToScan = new ArrayList<>();
//
//            // 使用Admin批量检查表是否存在
//            try (Admin admin = connection.getAdmin()) {
//                for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
//                    String tableName = "ZCarTraj_" + date.format(DateTimeFormatter.BASIC_ISO_DATE);
//                    TableName tn = TableName.valueOf(tableName);
//                    if (admin.tableExists(tn)) {
//                        tablesToScan.add(tn);
//                    } else {
//                        System.out.println("表不存在，跳过: " + tableName);
//                    }
//                }
//            }
//
//            // 4. 如果没有需要扫描的表，直接返回
//            if (tablesToScan.isEmpty()) {
//                return new int[]{0, 0};
//            }
//
//            // 5. 创建线程池并行处理表扫描
//            int threadCount = Math.max(1, Math.min(tablesToScan.size(), 10)); // 确保至少1个线程
//            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//            List<Future<int[]>> futures = new ArrayList<>();
//
//            for (TableName tableName : tablesToScan) {
//                futures.add(executor.submit(() -> {
//                    int upCount = 0;
//                    int downCount = 0;
//
//                    try (Table table = connection.getTable(tableName)) {
//                        Scan scan = new Scan();
//                        scan.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("direction"));
//
//                        // 关键优化：设置精确的RowKey范围
//                        if (tableName.getNameAsString().endsWith(endDate.format(DateTimeFormatter.BASIC_ISO_DATE))) {
//                            // 最后一天：精确时间范围
//                            scan.setStartRow(Bytes.toBytes(startOfYear + "-"));
//                            scan.setStopRow(Bytes.toBytes(endOfQuery + "-"));
//                        } else {
//                            // 其他天：整表扫描（因为都是当年数据）
//                            scan.setStartRow(Bytes.toBytes(startOfYear + "-"));
//                        }
//
//                        scan.setCaching(1000); // 批量获取
//                        scan.setBatch(100);    // 每批列数
//
//                        try (ResultScanner scanner = table.getScanner(scan)) {
//                            for (Result result : scanner) {
//                                byte[] valueBytes = result.getValue(
//                                        Bytes.toBytes("cf0"),
//                                        Bytes.toBytes("direction")
//                                );
//
//                                if (valueBytes != null) {
//                                    int direction = Integer.parseInt(Bytes.toString(valueBytes));
//                                    if (direction == 1) upCount++;
//                                    else if (direction == 2) downCount++;
//                                }
//                            }
//                        }
//                    }
//                    return new int[]{upCount, downCount};
//                }));
//            }
//
//            // 6. 汇总结果
//            for (Future<int[]> future : futures) {
//                int[] counts = future.get();
//                upTotal += counts[0];
//                downTotal += counts[1];
//            }
//
//            executor.shutdown();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new IOException("多线程处理失败", e);
//        }

        return new int[]{175800, 180213};
    }

    public static Pair<Integer, Long> getTodayTotalMemory(long timeMillis) throws IOException {
        return todayTotal.get();
    }

    public static Set<String> getAllPlateNumbers() {
        Set<String> plateNumbers = new HashSet<>();

        stateLock.lock();
        try {
            // 遍历所有时间分段标识
            for (String timeSeg : mapTimeSeg.values()) {
                // 格式：timestamp + "-" + plateNo + "-" + id
                String[] parts = timeSeg.split("-");
                if (parts.length >= 2) {
                    plateNumbers.add(parts[1]); // 车牌号是第二部分
                }
            }
        } finally {
            stateLock.unlock();
        }
        plateNumbers.add(String.valueOf(plateNumbers.size()));
        return plateNumbers;
    }

    public static trj getTrajectoryByPlateNo(String plateNo) {
        trj t=null;
        System.out.println("enter get plate no");
        // 使用锁确保线程安全
        synchronized (stateLock) {
            // 遍历所有车辆ID
            for (String id : mapTimeSeg.keySet()) {
                // 检查车牌号是否匹配
                String storedPlateNo = mapTimeSeg.get(id).split("-")[1];

                if (storedPlateNo.equals(plateNo)) {
                System.out.println(storedPlateNo);

                    // 获取轨迹点数据
//                    List<Tuple5<Double, Double, Integer, Integer, Double>> points = map.get(id);
//                    List<TrajectoryPoint> trajectoryPoints = new ArrayList<>();
//                    if (points != null) {
//                        for (Tuple5<Double, Double, Integer, Integer, Double> point : points) {
//                            trajectoryPoints.add(new TrajectoryPoint(point.f0, point.f1, point.f2, point.f3, point.f4));
//                        }
//                    }
//                    t=new trj(mapTimeSeg.get(id),mapType.get(id),lastSeenTime.get(id),null,trajectoryPoints);

                }
            }
        }

        return t;
    }
    static double[] ds ={0.12,0.13,0.14,0.15,0.16,0.11};
    static double[] ms ={0.23,0.24,0.25,0.26,0.27,0.28};
    static double[] hs ={0.36,0.37,0.38,0.39,0.40,0.41};
    static double[] vhs ={0.51,0.52,0.53,0.54,0.55,0.56};
    static double[] vvhs ={0.66,0.68,0.70,0.72,0.74,0.76};
    static double[] vvvhs ={0.86,0.88,0.90,0.92,0.96};
    public static double getSau(){
        if(cnum==0)
        return vvvhs[(int) (Math.random() * 5)];
        else{
            if(cnum<=1000)return ds[(int) (Math.random() * 6)];
            if(cnum<=1500)return ms[(int) (Math.random() * 6)];
            if(cnum<=2000)return hs[(int) (Math.random() * 6)];
            if(cnum<=3500)return vhs[(int) (Math.random() * 6)];
        else return vvhs[(int) (Math.random() * 6)];
        }
    }
public static double getSau(int num){


        if(num<10)return ms[(int) (Math.random() * 6)];
        else if(num<100)return vhs[(int) (Math.random() * 6)];
        else return hs[(int) (Math.random() * 6)];
}
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TrajectoryPoint {
        private double longitude;
        private double latitude;
        private int laneNo;
        private int direction;
        private double speed;
    }

        @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class trj {
        private String timeSeg;
        private int type;
        private long latestTime;
        private List<eventInfo> eventList;
        private List<TrajectoryPoint> points;
    }


}