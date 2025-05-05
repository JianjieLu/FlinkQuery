package com.ljj.flinkquery.demos.entity;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.Math.abs;

public class stakeEnvents {
        /**
     * StakeInfo
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class StakeInfo implements Serializable {
        private String stake;
        private double[] lnglat;
    }

        /**
     * StakeAssignment 会在主程序的static代码块中加载已知的gantry信息
     */
    @Getter
    @Setter
    public static class StakeAssignment implements Serializable {
        private static final double EARTH_RADIUS = 6371.393;
        private List<StakeInfo> stakeInfoList;

        public StakeAssignment(String excelFilePath) throws IOException {
            // 从Excel文件加载卡口信息
            this.stakeInfoList = loadCheckpointsFromJSON(excelFilePath);
        }

        private List<StakeInfo> loadCheckpointsFromJSON(String filePath) throws IOException {
            // 读取JSON
            String jsonString = readFileContent(filePath);

            // 解析 JSON 数据为 List<StakeInfo>
            List<StakeInfo> stakeInfoList = JSON.parseObject(jsonString, new TypeReference<List<StakeInfo>>() {});

            // 按纬度由大到小排序
            stakeInfoList.sort((o1, o2) -> Integer.compare(stakeToMileage(o1.getStake()), stakeToMileage(o2.getStake())));

            return stakeInfoList;
        }

        public String findInsertionIndex(double lng, double lat) {
            int left = 0;
            int right = stakeInfoList.size() - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                double[] coordinate = stakeInfoList.get(mid).getLnglat();
                double midDistance = calculateDistance(coordinate[0], coordinate[1], lng, lat) * 1000;

                if (midDistance <= 0.5) {
                    return stakeInfoList.get(mid).getStake();
                } else if (coordinate[1] < lat) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            if(left== stakeInfoList.size())left--;
            if(abs(stakeInfoList.get(left).getLnglat()[0]-lng)>0.01&&abs(stakeInfoList.get(left).getLnglat()[1]-lat)>0.01){
                System.out.println("abs1:  "+abs(stakeInfoList.get(left).getLnglat()[0]-lng));
                System.out.println("abs2:  "+abs(stakeInfoList.get(left).getLnglat()[1]-lat));
                return null;}
            return stakeInfoList.get(left).getStake();
        }

        public static double calculateDistance(double lon1, double lat1, double lon2, double lat2) {
            // 将角度转换为弧度
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);

            // Haversine 公式
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(lat1) * Math.cos(lat2) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = EARTH_RADIUS * c;

            return distance;
        }

        private String readFileContent(String filePath) {
            try {
                // 使用 Files.readAllBytes 方法读取文件内容
                byte[] bytes = Files.readAllBytes(Paths.get(filePath));
                return new String(bytes);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        private int stakeToMileage(String stakeId) {
            return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
        }
    }
}
