package com.ljj.flinkquery.demos.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class VehicleSeg {
    private Long carId;
    private String plateNo;
    private Integer plateColor;
    private float speedSum;
    private int direction;
    private int pointSum;
    private Integer originalType;
    private Integer vehicleType;
    private String specialFlag;
    private int laneNo;
    private double averageSpeed;
}