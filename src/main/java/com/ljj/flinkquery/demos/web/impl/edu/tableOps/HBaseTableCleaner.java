package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import java.io.IOException;

public class HBaseTableCleaner {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("用法: java HBaseTableCleaner <表名>");
            return;
        }
        String tableName = args[0];

        try {
            boolean result = totalOps.deleteTable(tableName);
            if (result) {
                System.out.println("✅ 表 " + tableName + " 已成功删除");
            } else {
                System.out.println("❌ 表不存在: " + tableName);
            }
        } catch (IOException e) {
            System.err.println("❗ 删除失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}