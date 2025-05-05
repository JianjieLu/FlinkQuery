package com.ljj.flinkquery.demos.web.impl.edu.tableOps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class addCF {
    public static void main(String[] args) {
        String tablename = args[0];//表名
        String cfname = args[1];
         Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tablename))) {
            Admin admin = connection.getAdmin();
            HColumnDescriptor newcf = new HColumnDescriptor(cfname);
            admin.addColumn(TableName.valueOf(tablename), newcf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    }
