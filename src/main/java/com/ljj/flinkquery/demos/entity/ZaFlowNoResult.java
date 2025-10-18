package com.ljj.flinkquery.demos.entity;

import com.ljj.flinkquery.demos.web.impl.edu.querys.TollStationFlowCalculator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
public class ZaFlowNoResult {
               private int code;
    private String message;
    private Map<String, TollStationFlowCalculator.TollStationFlow>  data;
    private boolean status;
}
