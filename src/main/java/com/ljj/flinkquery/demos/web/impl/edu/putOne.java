package com.ljj.flinkquery.demos.web.impl.edu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

//ctrl+o  查看extends
public class putOne {
    public static void main(String[] args) throws IOException {
         Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        // 获取连接
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("ljj"))) {
            Put put = new Put(Bytes.toBytes("1007"));
            put.addColumn(Bytes.toBytes("cf0"), Bytes.toBytes("name"), Bytes.toBytes("lujianjie"));
            table.put(put);
    }
}

}
