package com.ljj.flinkquery.demos.entity.watch;

import lombok.*;

@Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
public class sectionStartEndStake {
    private String sectionName;
    private int startStake;
    private int endStake;
}
