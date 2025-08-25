package com.ljj.flinkquery.demos.entity;

import lombok.*;

import java.util.List;
import java.util.Map;

@Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
public class upDownResult {
            private int code;
    private String message;
    private List<Map<String, Object>> data;
    private boolean status;
}
