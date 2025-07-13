package com.ljj.flinkquery.demos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SectionalBatchFlowResult {
    private int code;
private String message;
private List<SectionalFlowData> data;
private boolean status;
}
