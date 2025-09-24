package com.ljj.flinkquery.demos.entity.watch;

import com.ljj.flinkquery.demos.web.impl.edu.tableOps.totalOpsv3;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class carNumResult {
    private int code;
    private String message;
    private boolean status;
    List<totalOpsv3.TrafficResult> data;
}
