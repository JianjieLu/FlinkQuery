package com.ljj.flinkquery.demos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
public class SectionalFlowData {
    int upCount;
    int downCount;
    Map<Integer,Integer> upCountMap;
    Map<Integer,Integer> downCountMap;
    private SectionalFlowQuery inputParams;
}
