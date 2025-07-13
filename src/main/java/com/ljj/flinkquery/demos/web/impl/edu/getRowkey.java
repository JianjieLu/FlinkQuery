package com.ljj.flinkquery.demos.web.impl.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
//whu.edu.getRowkey
public class getRowkey {
    public static void main(String[] args) throws IOException {
          String tablename = args[0];//表名
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,100.65.38.36,100.65.38.37,100.65.38.38");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tablename))) {
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                // 获取行键
                byte[] rowKey = result.getRow();
                for (Cell cell : cells) {
                    System.out.println("cell-->"+Bytes.toString(CellUtil.cloneRow(cell)));
                }
                System.out.println("rowKey-->"+Bytes.toString(rowKey));
                scanner.close();
            }
        }
    }
}
