package com.ljj.flinkquery.demos.web.impl.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
//whu.edu.getByRowkey
public class getByRowkey {
    public static void main(String[] args) {
        String tablename = args[0];//表名
        String row=args[1];
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tablename))) {
            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("Row Key: " + Bytes.toString(result.getRow()));
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
