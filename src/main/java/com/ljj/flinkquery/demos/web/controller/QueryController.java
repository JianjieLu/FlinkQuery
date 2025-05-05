//package com.ljj.flinkquery.demos.web.controller;
//
//import com.ljj.flinkquery.demos.web.service.HBaseService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.*;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//
//@RestController
//public class QueryController {
//
//    @Autowired
//    private HBaseService hbaseService;
//
//    @RequestMapping(value = "/createTable", method = RequestMethod.GET)
//    @ResponseBody
//    public ResponseEntity<String> createTable(
//            @RequestParam("tableName") String tableName,
//            @RequestParam("columnFamilies") String columnFamilies) {
//
//        try {
//            List<String> columnFamilyList = Arrays.asList(columnFamilies.split(","));
//            hbaseService.createTable(tableName, columnFamilyList);
//            return ResponseEntity.ok("表创建成功: " + tableName);
//        } catch (IOException e) {
//            return ResponseEntity.status(500).body("HBase连接失败: " + e.getMessage());
//        } catch (IllegalArgumentException e) {
//            return ResponseEntity.status(400).body(e.getMessage());
//        }
//    }
//}