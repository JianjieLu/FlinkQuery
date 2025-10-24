package com.ljj.flinkquery.demos.entity.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class seventhData {
    private String stationId;
    private String name;
    private List<seventhPiece> dirStaList;
}
