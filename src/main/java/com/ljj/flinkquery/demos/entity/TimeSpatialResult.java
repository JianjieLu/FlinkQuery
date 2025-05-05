package com.ljj.flinkquery.demos.entity;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class TimeSpatialResult {

    private int code;
    private String message;
    private TimeSpatialData data;
    private boolean status;
}
