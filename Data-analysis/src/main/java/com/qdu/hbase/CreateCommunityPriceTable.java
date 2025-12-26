package com.qdu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateCommunityPriceTable {

    // 配置项（便于修改）
    private static final String NAMESPACE = "cjz";
    private static final String TABLE_NAME = "community_price_analysis"; // 新分析表名
    private static final String COLUMN_FAMILY = "info";
    private static final String ZOOKEEPER_QUORUM = "hadoop101";

    public static void main(String[] args) {
        try {
            System.out.println("开始创建 HBase 分析表: " + NAMESPACE + ":" + TABLE_NAME);
            createTable();
            System.out.println("表创建成功！");
        } catch (IOException e) {
            System.err.println("创建表时发生错误:");
            e.printStackTrace();
        }
    }

    private static void createTable() throws IOException {
        Configuration conf = getHBaseConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            // 创建命名空间（如果不存在）
            try {
                NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE).build();
                admin.createNamespace(ns);
                System.out.println("命名空间 " + NAMESPACE + " 创建成功");
            } catch (NamespaceExistException e) {
                System.out.println("命名空间 " + NAMESPACE + " 已存在");
            }

            TableName tableName = TableName.valueOf(NAMESPACE + ":" + TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(COLUMN_FAMILY))
                        .setMaxVersions(1);
                tableBuilder.setColumnFamily(cfBuilder.build());
                admin.createTable(tableBuilder.build());
                System.out.println("表 " + tableName + " 创建成功");
            } else {
                System.out.println("表 " + tableName + " 已存在");
            }
        }
    }

    private static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        conf.setInt("hbase.rpc.timeout", 60000);
        conf.setInt("hbase.client.operation.timeout", 60000);
        conf.setInt("hbase.client.scanner.timeout.period", 60000);
        return conf;
    }
}
