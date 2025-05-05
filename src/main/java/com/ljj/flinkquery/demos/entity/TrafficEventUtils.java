package com.ljj.flinkquery.demos.entity;

import com.alibaba.fastjson2.JSONArray;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import com.alibaba.fastjson2.JSON;

import java.util.List;

public class TrafficEventUtils {
    // 基类：交通事件类
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class TrafficEvent {
        private Integer eventId; // 事件id（唯一性）
        private String timeStamp; // 事件上报时间戳
        private Integer eventType; // 事件类型
        private String startStake; // 事件起始桩号
        private String endStake; // 事件结束桩号
        private Integer startMileage; // 事件范围起始里程
        private Integer endMileage; // 事件范围截止里程
        private Double startLongitude; // 事件范围起始经度
        private Double startLatitude; // 事件范围起始纬度
        private Double endLongitude; // 事件范围截止经度
        private Double endLatitude; // 事件范围截止纬度
        private String laneNo; // 事件或施工所在车道号
        private Integer direction; // 事件发生区域的行车方向
        private JSONArray carList; // 事件发生车辆集合
        private String eventLevel; // 事件等级
        private String eventDes; // 事件概要描述
        private String eventReason; // 事件原因
        private String eventPicPath; // 事件现场图片路径，多张逗号分隔
        private String eventVideoPath; // 事件现场视频路径
        private String eventSource; // 事件源
        private String sourceRemark; // 来源备注
        private String waySectionId; // 路段id
        private String waySectionName; // 路段名称
        private Boolean manualAudit; // 事件是否需要人工审核校验
        private Integer constructionVehicles; // 施工类事件，施工车数量
        private Integer constructionPerson; // 施工类事件，施工人员数量

        // toString方法
        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }


    // 继承自TrafficEvent的拥堵类
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class CongestionEvent extends TrafficEvent {
        private Integer congestionDuration; // 拥堵时间 (/min)
        private Double congestionIndex; // 拥堵指数
        private Double averageSpeed; // 平均车速 (km/h)
        private Double congestionMileage; // 拥堵总里程，精确到小数点后一位
        private Integer busCount; // 客车数量
        private Integer truckCount; // 货车数量
        private Integer chemicalCount; // 危化车数量
        private Integer heavyTruckCount; // 重型货车数量

        // toString方法
        @Override
        public String toString() {
            return super.toString();
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class CongestionEventResult extends TrafficEvent {
        private CongestionEvent c;
        private int code;
        private String message;
        private boolean status;

    }




}
