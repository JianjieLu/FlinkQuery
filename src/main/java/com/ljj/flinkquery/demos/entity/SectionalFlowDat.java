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
public class SectionalFlowDat {
    int direction;
    int total;
    List<SectionalFlowPiece> staList;
}
