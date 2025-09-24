package com.ljj.flinkquery.demos.entity.watch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * 路段车流量统计响应实体
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class flowNo {
    private Integer code;
    private String message;
    private SectionTrafficData data;
    boolean status;
    /**
 * 方向车流量信息
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public static class DirectionInfo {
    private Integer direction; // 行车方向，1：京珠向；2：珠京向
    private Integer total;     // 车辆总数
    private String yoy;        // 同比（去年同期）增长，保留两位小数
    private String mom;        // 环比（上一个统计周期）增长，保留两位小数
}
/**
 * 路段车流量数据
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public static class SectionTrafficData {
    private List<SectionInfo> sectionList;
}

/**
 * 路段信息
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public static class SectionInfo {
    private String name;        // 路段名称
    private String stake;       // 桩号范围
    private List<DirectionInfo> dirList; // 方向信息列表
}

}


