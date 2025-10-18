package com.ljj.flinkquery.demos.entity.watch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
//我现在要把返回类型改成
/**
 * 路段车流量统计响应实体
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
//    private Integer direction; // 行车方向，1：京珠向；2：珠京向
public class flowNoNew {
    private Integer code;
    private String message;
    private List<rank> data;
    boolean status;
    /**
 * 方向车流量信息
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public static class DirectionInfo {
    private String name;
    private String stake;
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
public static class rank {
    private int type;
    private List<DirectionInfo> sectionList;
}



}




