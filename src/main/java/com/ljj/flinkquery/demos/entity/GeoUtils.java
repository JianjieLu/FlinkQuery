package com.ljj.flinkquery.demos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

public class GeoUtils {
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
 public static class MBR {
    private double minX;
    private double maxX;
    private double minY;
    private double maxY;

//    // 构造函数（根据轨迹点计算MBR）
//    public MBR(List<Location> coordinates) {
//        this.minX = coordinates.stream().mapToDouble(Location::getLongitude).min().orElse(0);
//        this.maxX = coordinates.stream().mapToDouble(Location::getLongitude).max().orElse(0);
//        this.minY = coordinates.stream().mapToDouble(Location::getLatitude).min().orElse(0);
//        this.maxY = coordinates.stream().mapToDouble(Location::getLatitude).max().orElse(0);
//    }

    // 判断两个MBR是否相交
    public static boolean hasIntersection(MBR mbr1, MBR mbr2) {
        boolean xOverlap = (mbr1.minX <= mbr2.maxX) && (mbr1.maxX >= mbr2.minX);
        boolean yOverlap = (mbr1.minY <= mbr2.maxY) && (mbr1.maxY >= mbr2.minY);
        return xOverlap && yOverlap;
    }


}
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public static class tempMBR{
    private double X1;
    private double X2;
    private double Y1;
    private double Y2;
}
}

