package com.ljj.flinkquery.demos.entity.watch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
 @Getter
 @AllArgsConstructor
@NoArgsConstructor
 public class BatchQueryRequest {
     // getters 和 setters
     private List<String> stationIds;
    private String beginTime;
    private String endTime;
    private int level;


 }