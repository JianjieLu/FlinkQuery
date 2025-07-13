package com.ljj.flinkquery.demos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SectionalFlowQuery {
    private Long startTime;
    private Long endTime;
    private String startMileage;
    private String endMileage;
    private Double Longitude1;
    private Double Latitude1;
    private Double Longitude2;
    private Double Latitude2;
    private Integer level;

    // 构造器/getters/setters
}