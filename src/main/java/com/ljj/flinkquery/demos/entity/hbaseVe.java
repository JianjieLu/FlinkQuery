package com.ljj.flinkquery.demos.entity;

import lombok.*;

import java.util.Map;
public class hbaseVe {
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
         public static class VehicleSeg {
            private String plateNo;
            private Long carId;
            private Float speedSum;
            private Float aveSpeed;   // 新增平均速度
            private Integer direction; // 方向
            private Integer pointSum;
            private Integer originalType = null;
            private Integer vehicleType = null;
            private String specialFlag = null;
        }


        @Getter
        @Setter
        @NoArgsConstructor
        @AllArgsConstructor
        @ToString
        public static class VehicleSegAccumulator {
            private Long timeStamp;
            private Integer stakeNum;
            // direction = 1
            private Map<Long, VehicleSeg> vehicleSegMapD1;
            // direction = 2
            private Map<Long, VehicleSeg> vehicleSegMapD2;
        }
}

