package com.ljj.flinkquery.demos.web.controller;

import com.ljj.flinkquery.demos.entity.*;
import com.ljj.flinkquery.demos.entity.TrafficEventUtils.*;
import com.ljj.flinkquery.demos.web.service.HBaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
@RestController
public class HBaseController {

    @Autowired
    private HBaseService hbaseService;

    @RequestMapping(value = "/createTable", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<String> createTable(
            @RequestParam("tableName") String tableName,
            @RequestParam("columnFamilies") String columnFamilies) {
        log.info("Create table " + tableName);
        try {
            List<String> columnFamilyList = Arrays.asList(columnFamilies.split(","));
            hbaseService.createTable(tableName, columnFamilyList);
            return ResponseEntity.ok("表创建成功: " + tableName);
        } catch (IOException e) {
            return ResponseEntity.status(500).body("HBase连接失败: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(400).body(e.getMessage());
        }
    }

      @RequestMapping(value = "/getByRowKey", method = RequestMethod.GET)
    @ResponseBody
    public void getByRowKey(
            @RequestParam("tableName") String tableName,
            @RequestParam("rowKey") String rowKey) {

        try {
            hbaseService.getByRowKey(tableName, rowKey);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
      }
      @RequestMapping(value = "/empty", method = RequestMethod.GET)
    @ResponseBody
    public void empty(@RequestParam("tableName") String tableName, @RequestParam("rowKey") String rowKey) {
        try {
            hbaseService.getEmpty(tableName, rowKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
      }

      @RequestMapping(value = "/getBySkateID", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public String getBySkateID(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam("startMileage") String startMileage, @RequestParam("endMileage") String endMileage) {
        String res;

          try {
              res=hbaseService.getBySkateID(startTime, endTime,startMileage, endMileage);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
          return res;
      }
   @PostMapping("/getBatchSectionalFlow")
    public SectionalBatchFlowResult getBatchSectionalFlow(
        @RequestBody List<SectionalFlowQuery> queries) {

        return hbaseService.getBatchSectionalFlow(queries);
    }

   @RequestMapping(value = "/getSectionalFlow", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public SectionalFlowResult getSectionalFlow(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam(value = "startMileage",required = false) String startMileage, @RequestParam(value = "endMileage",required = false) String endMileage, @RequestParam(value = "Longitude1",required = false)Double Longitude1, @RequestParam(value = "Latitude1",required = false) Double Latitude1, @RequestParam(value = "Longitude2",required = false)Double Longitude2, @RequestParam(value = "Latitude2",required = false)Double Latitude2,@RequestParam(value="level",defaultValue = "0") Integer level) {
        SectionalFlowResult res;

          try {
              res=hbaseService.getSectionalFlow(startTime, endTime,startMileage, endMileage,Longitude1, Latitude1,Longitude2, Latitude2,level);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
          return res;
      }
         @RequestMapping(value = "/getStFlow", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public jizhanResult getStFlow(@RequestParam("stId") String stId,@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime) {
        jizhanResult res;

          try {
              res=hbaseService.getStFlow(stId,startTime, endTime);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
          return res;
      }

      @RequestMapping(value = "/getByTimeSpatial", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public TimeSpatialResult getByTimeSpatial(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam(value = "startMileage",required = false) String startMileage, @RequestParam(value = "endMileage",required = false) String endMileage, @RequestParam(value = "Longitude1",required = false)Double Longitude1, @RequestParam(value = "Latitude1",required = false) Double Latitude1, @RequestParam(value = "Longitude2",required = false)Double Longitude2, @RequestParam(value = "Latitude2",required = false)Double Latitude2) {
        long currentTime = System.currentTimeMillis();
        TimeSpatialResult res;
          try {
              res=hbaseService.getByTimeSpatial(startTime, endTime,startMileage, endMileage,Longitude1, Latitude1,Longitude2, Latitude2);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
        long currentTime1 = System.currentTimeMillis();
          System.out.println(currentTime1 - currentTime);
          return res;
      }
      @RequestMapping(value = "/getByTimeSpatialWithID", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public TimeSpatialResult getByTimeSpatialWithID(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam(value = "startMileage",required = false) String startMileage, @RequestParam(value = "endMileage",required = false) String endMileage, @RequestParam(value = "Longitude1",required = false)Double Longitude1, @RequestParam(value = "Latitude1",required = false) Double Latitude1, @RequestParam(value = "Longitude2",required = false)Double Longitude2, @RequestParam(value = "Latitude2",required = false)Double Latitude2) {
        long currentTime = System.currentTimeMillis();
        TimeSpatialResult res;
          try {
              res=hbaseService.getByTimeSpatialWithID(startTime, endTime,startMileage, endMileage,Longitude1, Latitude1,Longitude2, Latitude2);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
        long currentTime1 = System.currentTimeMillis();
          System.out.println(currentTime1 - currentTime);
          return res;
      }
      @RequestMapping(value = "/getCongestionEvent", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public List<CongestionEventResult> getCongestionEvent(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam(value = "startMileage",required = false) Integer startMileage, @RequestParam(value="endMileage",required = false) Integer endMileage, @RequestParam(value="Longitude1",required = false)Double Longitude1, @RequestParam(value="Latitude1",required = false) Double Latitude1, @RequestParam(value = "Longitude2",required = false)Double Longitude2, @RequestParam(value = "Latitude2",required = false)Double Latitude2,@RequestParam("direction")int direction) {
        long currentTime = System.currentTimeMillis();
        List<CongestionEventResult> res;
          try {
              res=hbaseService.getCongestionEvent(startTime, endTime,startMileage, endMileage,Longitude1, Latitude1,Longitude2, Latitude2,direction);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }

        long currentTime1 = System.currentTimeMillis();
          System.out.println("耗时："+(currentTime1 - currentTime));
          return res;
      }

            @RequestMapping(value = "/getTrafficCrowdedEventById", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public CongestionEvent getCongestionEventById(@RequestParam("eventId") int eventId) {
        long currentTime = System.currentTimeMillis();
        CongestionEvent res;
          try {
              res=hbaseService.getCongestionEventById(eventId);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
        TimeSpatialResult badreq=new TimeSpatialResult(0,"失败",null,true);

        long currentTime1 = System.currentTimeMillis();
          System.out.println(currentTime1 - currentTime);
          return res;
      }

      @RequestMapping(value = "/getByLongLati", method = RequestMethod.GET)
      @ResponseBody
      //@RequestParam("tableName") String tableName,
    public String getByLongLati(@RequestParam("startTime") Long startTime, @RequestParam("endTime") Long endTime, @RequestParam("Longitude1")Double Longitude1, @RequestParam("Latitude1") Double Latitude1,@RequestParam("Longitude2")Double Longitude2,@RequestParam("Latitude2")Double Latitude2) {
        String res;
          try {
              res=hbaseService.getByLongLati(startTime, endTime,Longitude1, Latitude1,Longitude2, Latitude2);
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
          return res;
      }
}

//车流量   桩号，经纬度，起止时间，平均车速，交通饱和度
