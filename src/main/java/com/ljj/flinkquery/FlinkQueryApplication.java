package com.ljj.flinkquery;

import com.ljj.flinkquery.demos.entity.data.Utils;
import com.ljj.flinkquery.demos.entity.stat;
import com.ljj.flinkquery.demos.web.impl.myTools;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import static com.ljj.flinkquery.demos.entity.data.Utils.*;



import static com.ljj.flinkquery.demos.entity.data.Utils.convertToTimestampMillis;
@SpringBootApplication
public class FlinkQueryApplication {

    public static final ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();
    public static RedisTemplate<String, String> redisTemplate;//60s内所有数据
    public static RedisTemplate<String, String> redisTemplate1;//10s内所有数据
    static int upcount = 0;
    int downcount = 0;
    public static void main(String[] args) throws Exception {
        SpringApplication.run(FlinkQueryApplication.class, args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(6);

        // 配置 KafkaSource
        String brokers = "100.65.38.40:9092";
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

        // 保存 flatMap 操作后的结果
        DataStream<PathPoint> flatMapStream = unionStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String jsonString, Collector<PathPoint> out) {
                        try {

                            JSONObject jsonObject = JSON.parseObject(jsonString);
//                            System.out.println(1);
                            for(PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class)) {

                                if (!ppoint.getStakeId().isEmpty()) {
                                    // 这里暂时将vt当作ot
                                    Integer vt = ppoint.getVehicleType();
//                                    System.out.println("vt:" + vt);
//                                    if(vt == null)
//                                        continue;
//                                    else if (!(vt == 1 || vt == 3 || vt == 7) && !(vt == 2 || vt == 8 || vt == 10 || vt == 11 ) && !(vt >= 170 && vt <= 183))
//                                        continue;
                                    ppoint.setOriginalType(vt);
                                    ppoint.setTimeStamp(jsonObject.getString("timeStamp"));
                                    out.collect(ppoint);
                                }
                            }

                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
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
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
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
        // 创建集群配置
        RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(
            Arrays.asList(
                "100.65.38.139:8001",
                "100.65.38.140:8002",
                "100.65.38.141:8003",
                "100.65.38.142:8004",
                "100.65.38.36:8005",
                "100.65.38.37:8006"
            )
        );
        clusterConfig.setPassword("123456");  // 设置密码

        // 配置客户端选项
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
            .commandTimeout(Duration.ofSeconds(2))
            .build();

        // 创建集群连接工厂
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
            clusterConfig,
            clientConfig
        );
        factory.afterPropertiesSet();

        // 创建Redis模板
        redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(factory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        redisTemplate.afterPropertiesSet();

         // 初始化第二个 RedisTemplate（3秒）
    redisTemplate1 = new RedisTemplate<>();
    redisTemplate1.setConnectionFactory(factory); // 复用同一个连接工厂
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
                10, TimeUnit.SECONDS // 设置10秒过期
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
}
