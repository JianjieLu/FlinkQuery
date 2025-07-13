package com.ljj.flinkquery.demos.entity.newFive;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;
@AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
public class totalResult {
    firstResult first;
    secondResult second;
    int total;
    double annualUpAverageNum;
    double annualDownAverageNum;
    Map<String, Map<Integer, Integer>> flowTendency;
}
