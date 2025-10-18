package com.ljj.flinkquery.demos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
public class jizhanResult {
    private int code;
    private String message;
    private jizhanUpDownCountData data;
    private boolean status;
    private long time;
}
