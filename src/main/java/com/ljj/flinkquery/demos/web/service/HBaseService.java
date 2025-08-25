package com.ljj.flinkquery.demos.web.service;
import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.entity.TrafficEventUtils.*;

import java.io.IOException;
import java.util.List;

public interface HBaseService {
    void getByRowKey(String tableName, String rowKey) throws IOException;
    void createTable(String tableName, List<String> columnFamilies) throws IOException;
    String getBySkateID(Long startTime, Long endTime, String startMileage, String endMileage) throws IOException;
    void getEmpty(String tableName, String rowKey);
    String getByLongLati(Long startTime,Long endTime, Double Longitude1,Double Latitude1,Double Longitude2,Double Latitude2) throws IOException;
    TimeSpatialResult getByTimeSpatialWithID(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException;
    List<CongestionEvent> getCrowdedInfo(Long startTime, Long endTime, String startMileage, String endMileage) throws IOException;
    CongestionEvent getCongestionEventById(int eventId);
    List<CongestionEventResult> getCongestionEvent(Long startTime, Long endTime, Integer startMileage, Integer endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2,int direction) throws IOException;



    jizhanResult getStFlow(String stId,Long startTime,Long endTime) throws IOException;
    SectionalFlowResult getSectionalFlow(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2, Integer level) throws IOException;
    SectionalBatchFlowResult getBatchSectionalFlow(List<SectionalFlowQuery> queries);
    TimeSpatialResult getByTimeSpatial(Long startTime, Long endTime, String startMileage, String endMileage, Double Longitude1, Double Latitude1, Double Longitude2, Double Latitude2) throws IOException;

}