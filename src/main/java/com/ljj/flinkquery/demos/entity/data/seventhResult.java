package com.ljj.flinkquery.demos.entity.data;

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
public class seventhResult {
    private int code;
    private String message;
    private boolean status;
    List<seventhData> data;
}
