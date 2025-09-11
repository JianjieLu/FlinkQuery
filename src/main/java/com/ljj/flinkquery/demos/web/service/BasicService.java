package com.ljj.flinkquery.demos.web.service;

import com.ljj.flinkquery.FlinkQueryApplication;
import com.ljj.flinkquery.demos.entity.watch.sectionLosResult;
import com.ljj.flinkquery.demos.entity.watch.zaEachSitResult;
import org. json. JSONObject;
import com.ljj.flinkquery.demos.entity.newFive.firstResult;
import com.ljj.flinkquery.demos.entity.newFive.secondResult;
import com.ljj.flinkquery.demos.entity.newFive.totalResult;
import com.ljj.flinkquery.demos.entity.upDownResult;
import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOps;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface BasicService {
Map<Integer, Integer> getVehicleTypesByDuration(Long startTime, Long endTime);
    String getByStake(Long startTime, Long endTime, String startMileage, String endMileage);
 List<String> getRowKeysByQualifier(String tableName, String cf, String quali) throws IOException ;
     List<totalOps.VehicleData> getAllVehicleData(String tableName,List<String> qualifiers) throws IOException;
boolean deleteTable(String tableName) throws IOException;
List<totalOps.TrajData> getTrajInTimeRange(Long startTime, Long endTime) throws IOException;
    List<totalOps.VehicleData> getVehicleDataInTimeRange(Long startTime,Long endTime, List<String> qualifier) throws IOException ;
Map<String, Map<Integer, Map<Integer, Integer>>> getHourlyTrafficStatistics(long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException;

upDownResult getUpDownChargerByDuration(String stationId, String startTime, String endTime);
totalResult getHolyTotal(Long timestamp) throws IOException, ExecutionException, InterruptedException;
List<upDownResult> getBatchUpDownCharger(List<String> stationIds,String beginTime,String endTime);


sectionLosResult sectionLOS(String beginTime,String endTime ,String startStake,String endStake) throws IOException;
sectionLosResult zaSectionLOS(String beginTime,String endTime ,String facilitiesId) throws IOException;
List<zaEachSitResult> zaEachSit(List<String> stationIds,String beginTime,String endTime,int level) throws Exception;

FlinkQueryApplication.trj getTrajectoryByPlateNo(String plateNo) throws IOException;
Set<String> getAllPlateNumbers();
double[] test1(Long timestamp) throws IOException;
secondResult test2(Long timestamp);
Map<String, Map<Integer, Map<Integer, Integer>>> test3(Long timestamp) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException;

}
