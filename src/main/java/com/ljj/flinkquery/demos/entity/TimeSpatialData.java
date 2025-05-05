package com.ljj.flinkquery.demos.entity;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class TimeSpatialData {

    //平均速度
    private double totalAverageSpeed;
    private double upAverageSpeed;
    private double downAverageSpeed;

    //车数
    private int totalCount;
    private int upCount;
    private int downCount;

    //交通饱和度
    private double trafficSaturation;

    //车密度-  车数/公里数
    private double vehicleDensity;

    //拥塞指数
    private double totalCongestionIndex;
    private double upCongestionIndex;
    private double downCongestionIndex;


    private int upBusCount;
    private int upTrackCount;
    private int upChemicalCount;
    private int upHeavyTrackCount;

    private int downBusCount;
    private int downTrackCount;
    private int downChemicalCount;
    private int downHeavyTrackCount;

    //客货比
    private double busTrackVal;
    private double upBusTrackVal;
    private double downBusTrackVal;



    private double  zaAverageSpeed;
    private double   zaCount;
    private double   zaTrafficSaturation;
    private double   zaVehicleDensity;
    private double   zaCongestionIndex;
    private int   zaBusCount;
    private int   zaTrackCount;
    private int   zaChemicalCount;
    private int   zaHeavyTrackCount;
    private double zaBusTrackVal;

}
