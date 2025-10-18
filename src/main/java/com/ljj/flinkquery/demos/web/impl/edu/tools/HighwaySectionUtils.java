package com.ljj.flinkquery.demos.web.impl.edu.tools;
import com.ljj.flinkquery.demos.entity.watch.sectionStartEndStake;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class HighwaySectionUtils {

    // 定义路段信息
    public static final HighwaySection[] SECTIONS = {
        new HighwaySection("鄂北", "K1016+020", "大新", "K1030+448"),
        new HighwaySection("大新", "K1030+448", "大悟", "K1043+400"),
        new HighwaySection("大悟", "K1043+400", "阳平", "K1058+300"),
        new HighwaySection("阳平", "K1058+300", "大悟南枢纽", "K1062+700"),
        new HighwaySection("大悟南枢纽", "K1062+700", "小河", "K1075+200"),
        new HighwaySection("小河", "K1075+200", "孝昌", "K1092+242"),
        new HighwaySection("孝昌", "K1092+242", "桃花驿", "K1110+002"),
        new HighwaySection("桃花驿", "K1110+002", "孝南枢纽", "K1115+583"),
        new HighwaySection("孝南枢纽", "K1115+583", "孝感东", "K1122+200"),
        new HighwaySection("孝感东", "K1122+200", "府河", "K1129+200"),
        new HighwaySection("府河", "K1129+200", "灯塔枢纽", "K1140+371"),
        new HighwaySection("灯塔枢纽", "K1140+371", "东西湖枢纽", "K1148+571"),
        new HighwaySection("东西湖枢纽", "K1148+571", "武汉北", "K1153+992"),
        new HighwaySection("武汉北", "K1153+992", "蔡甸枢纽", "K1163+000"),
        new HighwaySection("蔡甸枢纽", "K1163+000", "天鹅湖", "K1168+100"),
        new HighwaySection("天鹅湖", "K1168+100", "武汉西枢纽", "K1173+535")
    };

    /**
     * 根据起点桩号和终点桩号获取经过的路段
     *
     * @param startStake 起点桩号，格式如 "K1234+001"
     * @param endStake 终点桩号，格式如 "K1235+020"
     * @return 经过的路段列表，每个元素为 "起点名称-终点名称" 格式
     */
    public static List<sectionStartEndStake> getPassedSectionStartEndStake(String startStake, String endStake) {
        List<sectionStartEndStake> passedSections = new ArrayList<>();

        // 转换桩号为数字
        int startNum = stakeToNumber(startStake);
        int endNum = stakeToNumber(endStake);

        // 确保起点小于终点
        if (startNum > endNum) {
            int temp = startNum;
            startNum = endNum;
            endNum = temp;
        }

        // 检查每个路段是否与给定区间有重叠
        for (HighwaySection section : SECTIONS) {
            int sectionStart = stakeToNumber(section.startStake);
            int sectionEnd = stakeToNumber(section.endStake);

            // 判断两个区间是否有重叠
            if (!(endNum < sectionStart || startNum > sectionEnd)) {
                passedSections.add(new sectionStartEndStake(section.startName + "-" + section.endName,stakeToNumber4(section.startStake),stakeToNumber4(section.endStake)));
            }
        }

        return passedSections;
    }
public static List<String> getPassedSections(String startStake, String endStake) {
        List<String> passedSections = new ArrayList<>();

        // 转换桩号为数字
        int startNum = stakeToNumber(startStake);
        int endNum = stakeToNumber(endStake);

        // 确保起点小于终点
        if (startNum > endNum) {
            int temp = startNum;
            startNum = endNum;
            endNum = temp;
        }

        // 检查每个路段是否与给定区间有重叠
        for (HighwaySection section : SECTIONS) {
            int sectionStart = stakeToNumber(section.startStake);
            int sectionEnd = stakeToNumber(section.endStake);

            // 判断两个区间是否有重叠
            if (!(endNum < sectionStart || startNum > sectionEnd)) {
                passedSections.add(section.startName + "-" + section.endName);
            }
        }

        return passedSections;
    }
    /**
     * 获取所有路段的起始结束桩号列表
     *
     * @return 路段信息列表，包含每个路段的起始结束桩号
     */
    public static List<SectionStakeInfo> getAllSectionStakes() {
        List<SectionStakeInfo> sectionStakes = new ArrayList<>();

        for (HighwaySection section : SECTIONS) {
            sectionStakes.add(new SectionStakeInfo(
                section.startName + "-" + section.endName,
                section.startStake,
                section.endStake
            ));
        }

        return sectionStakes;
    }

    /**
     * 将桩号字符串转换为数字
     *
     * @param stakeStr 桩号字符串，格式如 "K1234+001"
     * @return 桩号对应的数字值（以米为单位）
     */
    private static int stakeToNumber(String stakeStr) {
        // 移除'K'和'+'，然后分割公里和米部分
        String cleaned = stakeStr.replace("K", "").replace("+", " ");
        String[] parts = cleaned.split(" ");

        int kilometers = Integer.parseInt(parts[0]);
        int meters = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;

        return kilometers * 1000 + meters;
    }
       private static int stakeToNumber4(String stakeStr) {
        // 移除'K'和'+'，然后分割公里和米部分
        String cleaned = stakeStr.replace("K", "").replace("+", " ");
        String[] parts = cleaned.split(" ");

        int kilometers = Integer.parseInt(parts[0]);
        int meters = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;

        return kilometers ;
    }

    /**
     * 内部类：表示一个高速公路路段
     */
    @AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
    public static class HighwaySection {
        String startName;
        String startStake;
        String endName;
        String endStake;

    }

    /**
     * 内部类：表示路段桩号信息
     */
    @AllArgsConstructor
    @Getter
@Setter
    public static class SectionStakeInfo {
        public final String sectionName;
        public final String startStake;
        public final String endStake;


        @Override
        public String toString() {
            return sectionName + ": " + startStake + " - " + endStake;
        }
    }

    // 测试方法
    public static void main(String[] args) {
        // 测试用例1：跨越多个路段
        String start1 = "K1016+020";
        String end1 = "K1035+000";
        System.out.println(end1.substring(0,2));
        List<String> sections1 = getPassedSections(start1, end1);
        System.out.println("从 " + start1 + " 到 " + end1 + " 经过的路段:");
        sections1.forEach(System.out::println);

        // 测试用例2：单个路段内
        String start2 = "K1120+000";
        String end2 = "K1130+000";
        List<String> sections2 = getPassedSections(start2, end2);
        System.out.println("\n从 " + start2 + " 到 " + end2 + " 经过的路段:");
        sections2.forEach(System.out::println);

        // 测试用例3：反向行驶
        String start3 = "K1170+000";
        String end3 = "K1150+000";
        List<String> sections3 = getPassedSections(start3, end3);
        System.out.println("\n从 " + start3 + " 到 " + end3 + " 经过的路段:");
        sections3.forEach(System.out::println);

        // 测试用例4：获取所有路段的桩号信息
        System.out.println("\n所有路段的桩号信息:");
        List<SectionStakeInfo> allSections = getAllSectionStakes();
        allSections.forEach(System.out::println);
    }
}