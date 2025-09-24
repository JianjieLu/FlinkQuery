package com.ljj.flinkquery.demos.web.controller;

import com.ljj.flinkquery.FlinkQueryApplication;
import com.ljj.flinkquery.demos.entity.watch.carNumResult;
import com.ljj.flinkquery.demos.entity.watch.flowNo;
import com.ljj.flinkquery.demos.entity.watch.sectionLosResult;
import com.ljj.flinkquery.demos.entity.watch.zaEachSitResult;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOpsv3;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org. json. JSONObject;
import com.ljj.flinkquery.demos.entity.SectionalBatchFlowResult;
import com.ljj.flinkquery.demos.entity.SectionalFlowQuery;
import com.ljj.flinkquery.demos.entity.newFive.firstResult;
import com.ljj.flinkquery.demos.entity.newFive.secondResult;
import com.ljj.flinkquery.demos.entity.newFive.totalResult;
import com.ljj.flinkquery.demos.entity.upDownResult;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps;
import com.ljj.flinkquery.demos.web.service.BasicService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
public class BasicController {

    @Autowired
    private BasicService basicService;

    @RequestMapping(value = "/getByDuration", method = RequestMethod.GET)
    @ResponseBody
    public Map<Integer, Integer> getVehicleTypesByDuration(
            @RequestParam("startTime") Long startTime,
            @RequestParam("endTime") Long endTime) {

        log.info("查询车辆类型分布: startTime={}, endTime={}", startTime, endTime);

        try {
            return basicService.getVehicleTypesByDuration(startTime, endTime);
        } catch (Exception e) {
            log.error("查询失败: ", e);
            throw new RuntimeException("车辆类型查询异常", e);
        }
    }

    @RequestMapping(value = "/getByStake", method = RequestMethod.GET)
    @ResponseBody
    //@RequestParam("tableName") String tableName,
    public String getByStake(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam("startMileage") String startMileage, @RequestParam("endMileage") String endMileage) {
        String res;

        try {
            res = basicService.getByStake(startTime, endTime, startMileage, endMileage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }


    @RequestMapping(value = "/getData", method = RequestMethod.GET)
    @ResponseBody
    //@RequestParam("tableName") String tableName,
    public List<String> getRowKeysByQualifier(@RequestParam("tableName") String tableName, @RequestParam("cf") String cf, @RequestParam("quali") String quali) throws IOException {
        return basicService.getRowKeysByQualifier(tableName, cf, quali);
    }

    @GetMapping("/getAll")
    public List<totalOps.VehicleData> getAllVehicleData(
            @RequestParam(value = "tableName", defaultValue = "vehicle_data") String tableName,
            @RequestParam(value = "qualifier", required = false) List<String> qualifiers) {

        try {
            log.info("查询HBase表数据: table={}, qualifiers={}", tableName, qualifiers);
            return basicService.getAllVehicleData(tableName, qualifiers);
        } catch (Exception e) {
            log.error("查询HBase数据失败", e);
            throw new RuntimeException("HBase查询异常: " + e.getMessage());
        }
    }


    @DeleteMapping("/getVehicleDataInTimeRange")
    public List<totalOps.VehicleData> getVehicleDataInTimeRange(
            @RequestParam("startTime") Long startTime,
            @RequestParam("endTime") Long endTime,
            @RequestParam("qualifier") List<String> qualifier) throws IOException {
        try {
            return basicService.getVehicleDataInTimeRange(startTime, endTime, qualifier);
        } catch (Exception e) {
            log.error("查询HBase数据失败", e);
            throw new RuntimeException("HBase查询异常: " + e.getMessage());
        }

    }


    //只需指定时间，会自动锁定表
    @RequestMapping(value = "/getTrajInTime", method = RequestMethod.GET)
    @ResponseBody
    public List<totalOps.TrajData> getTrajInTimeRange(
            @RequestParam("startTime") Long startTime,
            @RequestParam("endTime") Long endTime) {

        log.info("查询时间段内的轨迹数据: startTime={}, endTime={}", startTime, endTime);

        try {
            return basicService.getTrajInTimeRange(startTime, endTime);
        } catch (Exception e) {
            log.error("轨迹查询失败: ", e);
            throw new RuntimeException("轨迹数据查询异常", e);
        }
    }


    @GetMapping("/getHourlyTrafficStatistics")
    public Map<String, Map<Integer, Map<Integer, Integer>>> getHourlyTrafficStatistics(
            @RequestParam("timestamp") long timestamp) {

        log.info("获取每小时车流量统计: timestamp={}", timestamp);

        try {
            return basicService.getHourlyTrafficStatistics(timestamp);
        } catch (Exception e) {
            log.error("车流量统计失败: ", e);
            throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
        }
    }


    @GetMapping("/test1")
    public double[] test1(
            @RequestParam("timestamp") long timestamp) {

        log.info("获取每小时车流量统计: timestamp={}", timestamp);

        try {
            return basicService.test1(timestamp);
        } catch (Exception e) {
            log.error("车流量统计失败: ", e);
            throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
        }
    }

    @GetMapping("/test2")
    public secondResult test2(
            @RequestParam("timestamp") long timestamp) {

        log.info("获取每小时车流量统计: timestamp={}", timestamp);

        try {
            return basicService.test2(timestamp);
        } catch (Exception e) {
            log.error("车流量统计失败: ", e);
            throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
        }
    }

    @GetMapping("/test3")
    public Map<String, Map<Integer, Map<Integer, Integer>>> test3(
            @RequestParam("timestamp") long timestamp) {

        log.info("获取每小时车流量统计: timestamp={}", timestamp);

        try {
            return basicService.test3(timestamp);
        } catch (Exception e) {
            log.error("车流量统计失败: ", e);
            throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
        }
    }

    @GetMapping("/getHolyTotal")
    public totalResult getHolyTotal(
        @RequestParam("timeMillis") long timestamp) {

        log.info("获取每小时车流量统计: timestamp={}", timestamp);

        try {
            return basicService.getHolyTotal(timestamp);
        } catch (Exception e) {
            log.error("车流量统计失败: ", e);
            throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
        }
    }



@PostMapping("/getUpDownChargerByDuration")
public upDownResult getUpDownChargerByDuration(@RequestBody Map<String, String> requestParams) { // 改用 RequestBody 接收 JSON 参数
    String stationId = requestParams.get("stationId");
    String beginTime = requestParams.get("beginTime");
    String endTime = requestParams.get("endTime");
    try {
        return basicService.getUpDownChargerByDuration(stationId, beginTime, endTime);
    } catch (Exception e) {
        log.error("车流量统计失败: ", e);
        throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
    }
}
@PostMapping("/getBatchUpDownCharger")
public List<upDownResult> getBatchUpDownCharger(@RequestBody BatchQueryRequest request) {
    return basicService.getBatchUpDownCharger(
        request.getStationId(),
        request.getBeginTime(),
        request.getEndTime(),
        request.getLevel()
    );
}

@GetMapping("/plateNumbers")
public Set<String> getAllPlateNumbers() {
    return basicService.getAllPlateNumbers();
}


@GetMapping("/getTrj")
public FlinkQueryApplication.trj getTrajectoryByPlateNo(String plateNo) throws IOException {
        return basicService.getTrajectoryByPlateNo(plateNo);
}
@PostMapping("/sectionLOS")
public sectionLosResult sectionLOS(@RequestBody Map<String, String> requestParams) { // 改用 RequestBody 接收 JSON 参数
    String startStake = requestParams.get("startStake");
    String endStake = requestParams.get("endStake");
    String beginTime = requestParams.get("beginTime");
    String endTime = requestParams.get("endTime");
    try {
        return basicService.sectionLOS(beginTime, endTime, startStake,endStake);
    } catch (Exception e) {
        log.error("车流量统计失败: ", e);
        throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
    }
}

@PostMapping("/zaSectionLOS")
public sectionLosResult zaSectionLOS(@RequestBody Map<String, String> requestParams) { // 改用 RequestBody 接收 JSON 参数
    String facilitiesId = requestParams.get("facilitiesId");
    String beginTime = requestParams.get("beginTime");
    String endTime = requestParams.get("endTime");
    try {
        return basicService.zaSectionLOS(beginTime, endTime,facilitiesId);
    } catch (Exception e) {
        log.error("车流量统计失败: ", e);
        throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
    }
}

  @PostMapping("/zaEachSit")
    public zaEachSitResult zaEachSit(
         List<String> stationId,String beginTime,String endTime,int level) throws Exception {
//         return null;
        return basicService.zaEachSit(stationId,beginTime,endTime,level);
    }

    @PostMapping("/getFlowNo")
public flowNo getFlowNo(@RequestBody Map<String, String> requestParams) { // 改用 RequestBody 接收 JSON 参数
    String startStake = requestParams.get("startStake");
    String endStake = requestParams.get("endStake");
    String beginTime = requestParams.get("startTime");
    String endTime = requestParams.get("endTime");
    try {
        return basicService.getFlowNo(beginTime, endTime, startStake,endStake);
    } catch (Exception e) {
        log.error("车流量统计失败: ", e);
        throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
    }
}

    @PostMapping("/getCarNumber")
public carNumResult getCarNumber(@RequestBody Map<String, String> requestParams) { // 改用 RequestBody 接收 JSON 参数
    String startStake = requestParams.get("startStake");
    String endStake = requestParams.get("endStake");
    String beginTime = requestParams.get("startTime");
    String endTime = requestParams.get("endTime");
    int level= Integer.parseInt(requestParams.get("level"));
    try {
        return basicService.getCarNumber(beginTime, endTime, startStake,endStake,level);
    } catch (Exception e) {
        log.error("车流量统计失败: ", e);
        throw new RuntimeException("车流量统计异常: " + e.getMessage(), e);
    }
}
}
 @Setter
 @Getter
 @AllArgsConstructor
 class BatchQueryRequest {
     // getters 和 setters
     private List<String> stationId;
    private String beginTime;
    private String endTime;
    private int level;

    // 必须有无参构造函数
    public BatchQueryRequest() {}

 }
//车流量   桩号，经纬度，起止时间，平均车速，交通饱和度
