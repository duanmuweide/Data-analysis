package com.qdu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveToHBaseHouseDataMigration {

    // 连接配置信息 - 根据实际情况修改
    private static final String HIVE_JDBC_URL = "jdbc:hive2://master-pc:10000/dataanalysis";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";
    private static final String ZOOKEEPER_QUORUM = "master-pc";

    // HBase表和列族配置
    private static final String NAMESPACE = "dataanalysis";
    private static final String TABLE_NAME = "house_info_clean";
    private static final String COLUMN_FAMILY = "info";

    public static void main(String[] args) {
        try {
            System.out.println("开始房屋数据迁移任务...");

            // 1. 创建HBase表
            createHBaseTable();

            // 2. 迁移数据
            migrateDataFromHiveToHBase();

            System.out.println("数据迁移任务完成！");

        } catch (Exception e) {
            System.err.println("数据迁移过程中出现错误:");
            e.printStackTrace();
        }
    }

    /**
     * 创建HBase表
     */
    private static void createHBaseTable() throws IOException {
        System.out.println("开始创建HBase表...");

        Configuration hbaseConf = getHBaseConfiguration();

        // 使用全限定类名避免冲突
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf);
        Admin admin = hbaseConn.getAdmin();

        try {
            // 创建命名空间（如果不存在）
            try {
                NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE).build();
                admin.createNamespace(ns);
                System.out.println("命名空间 " + NAMESPACE + " 创建成功");
            } catch (NamespaceExistException e) {
                System.out.println("命名空间 " + NAMESPACE + " 已存在");
            }

            // 创建表
            TableName tableName = TableName.valueOf(NAMESPACE + ":" + TABLE_NAME);

            if (!admin.tableExists(tableName)) {
                // 创建表描述符
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);

                // 创建列族描述符
                ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                cfBuilder.setMaxVersions(1);  // 只保留一个版本

                // 设置列族
                tableBuilder.setColumnFamily(cfBuilder.build());

                // 创建表
                admin.createTable(tableBuilder.build());
                System.out.println("表 " + NAMESPACE + ":" + TABLE_NAME + " 创建成功");
            } else {
                System.out.println("表 " + NAMESPACE + ":" + TABLE_NAME + " 已存在");
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
            if (hbaseConn != null) {
                hbaseConn.close();
            }
        }
    }

    /**
     * 从Hive迁移数据到HBase
     */
    private static void migrateDataFromHiveToHBase() throws Exception {
        System.out.println("开始从Hive迁移数据到HBase...");

        // 连接Hive
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        java.sql.Connection hiveConn = DriverManager.getConnection(
                HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);

        // 查询Hive中的数据
        String query = "SELECT " +
                "rowkey, district, community, layout, orientation, " +
                "floor_num, decoration, elevator_int, area, price, " +
                "price_per_sqm, build_year, house_age " +
                "FROM house_info_clean";

        PreparedStatement stmt = hiveConn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();

        // 连接HBase
        Configuration hbaseConf = getHBaseConfiguration();
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf);
        Table table = hbaseConn.getTable(TableName.valueOf(NAMESPACE + ":" + TABLE_NAME));

        try {
            int count = 0;
            int batchSize = 100; // 批量处理大小
            java.util.List<Put> puts = new java.util.ArrayList<>();

            while (rs.next()) {
                count++;

                // 获取Hive数据
                String rowkey = rs.getString("rowkey");

                // 创建Put对象，使用rowkey作为HBase的行键
                Put put = new Put(Bytes.toBytes(rowkey));

                // 添加各个字段到对应的列
                addColumnIfNotNull(put, "district", rs.getString("district"));
                addColumnIfNotNull(put, "community", rs.getString("community"));
                addColumnIfNotNull(put, "layout", rs.getString("layout"));
                addColumnIfNotNull(put, "orientation", rs.getString("orientation"));
                addColumnIfNotNull(put, "decoration", rs.getString("decoration"));
                addColumnIfNotNull(put, "build_year", rs.getString("build_year"));

                // 数值类型字段
                addIntColumnIfNotNull(put, "floor_num", rs.getInt("floor_num"));
                addIntColumnIfNotNull(put, "elevator_int", rs.getInt("elevator_int"));
                addIntColumnIfNotNull(put, "area", rs.getInt("area"));
                addIntColumnIfNotNull(put, "price", rs.getInt("price"));
                addIntColumnIfNotNull(put, "price_per_sqm", rs.getInt("price_per_sqm"));
                addIntColumnIfNotNull(put, "house_age", rs.getInt("house_age"));

                puts.add(put);

                // 批量提交
                if (puts.size() >= batchSize) {
                    table.put(puts);
                    System.out.println("已处理 " + count + " 条记录");
                    puts.clear();
                }

                // 每1000条输出一次进度
                if (count % 1000 == 0) {
                    System.out.println("处理进度: " + count + " 条记录");
                }
            }

            // 提交剩余的数据
            if (!puts.isEmpty()) {
                table.put(puts);
                System.out.println("提交最后一批数据，共 " + puts.size() + " 条");
            }

            System.out.println("总共迁移了 " + count + " 条记录");

            // 验证数据完整性
            verifyDataIntegrity(count, hiveConn, hbaseConn);
        } finally {
            // 关闭资源
            if (table != null) {
                table.close();
            }
            if (hbaseConn != null) {
                hbaseConn.close();
            }
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (hiveConn != null) {
                hiveConn.close();
            }
        }
    }

    /**
     * 添加字符串类型的列（如果值不为空）
     */
    private static void addColumnIfNotNull(Put put, String column, String value) {
        if (value != null && !value.trim().isEmpty()) {
            put.addColumn(
                    Bytes.toBytes(COLUMN_FAMILY),
                    Bytes.toBytes(column),
                    Bytes.toBytes(value)
            );
        }
    }

    /**
     * 添加整数类型的列（如果值有效）
     */
    private static void addIntColumnIfNotNull(Put put, String column, int value) {
        put.addColumn(
                Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(column),
                Bytes.toBytes(String.valueOf(value))
        );
    }

    /**
     * 验证数据完整性
     */
    private static void verifyDataIntegrity(int expectedCount,
                                            java.sql.Connection hiveConn,
                                            org.apache.hadoop.hbase.client.Connection hbaseConn)
            throws Exception {
        System.out.println("开始验证数据完整性...");

        Table table = null;
        try {
            table = hbaseConn.getTable(TableName.valueOf(NAMESPACE + ":" + TABLE_NAME));

            // 1. 验证HBase中的记录数
            Scan scan = new Scan();
            scan.setCaching(1000);

            ResultScanner scanner = table.getScanner(scan);
            int hbaseCount = 0;
            for (Result result : scanner) {
                hbaseCount++;
            }
            scanner.close();

            System.out.println("HBase中实际记录数: " + hbaseCount);
            System.out.println("Hive中预期记录数: " + expectedCount);

            if (hbaseCount == expectedCount) {
                System.out.println("✓ 数据完整性验证通过！");
            } else {
                System.out.println("⚠ 警告: 数据数量不匹配！");
            }

            // 2. 随机抽样验证几条数据
            System.out.println("正在进行随机抽样验证...");
            String sampleQuery = "SELECT rowkey, district, price FROM house_info_clean LIMIT 5";
            PreparedStatement sampleStmt = hiveConn.prepareStatement(sampleQuery);
            ResultSet sampleRs = sampleStmt.executeQuery();

            while (sampleRs.next()) {
                String rowkey = sampleRs.getString("rowkey");
                String expectedDistrict = sampleRs.getString("district");
                String expectedPrice = String.valueOf(sampleRs.getInt("price"));

                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);

                if (!result.isEmpty()) {
                    String actualDistrict = Bytes.toString(result.getValue(
                            Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("district")));
                    String actualPrice = Bytes.toString(result.getValue(
                            Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("price")));

                    if (expectedDistrict != null && expectedDistrict.equals(actualDistrict) &&
                            expectedPrice.equals(actualPrice)) {
                        System.out.println("✓ 抽样验证通过 - RowKey: " + rowkey);
                    } else {
                        System.out.println("✗ 抽样验证失败 - RowKey: " + rowkey);
                        System.out.println("  期望: district=" + expectedDistrict + ", price=" + expectedPrice);
                        System.out.println("  实际: district=" + actualDistrict + ", price=" + actualPrice);
                    }
                } else {
                    System.out.println("✗ 数据未找到 - RowKey: " + rowkey);
                }
            }

            sampleRs.close();
            sampleStmt.close();

        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 获取HBase配置
     */
    private static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        conf.setInt("hbase.rpc.timeout", 60000);
        conf.setInt("hbase.client.operation.timeout", 60000);
        conf.setInt("hbase.client.scanner.timeout.period", 60000);
        conf.setInt("hbase.client.write.buffer", 2097152); // 2MB写缓冲区

        // 启用批处理优化
        conf.setBoolean("hbase.client.enable.automatic.flush", true);
        conf.setLong("hbase.client.flush.interval", 1000); // 1秒刷新间隔

        return conf;
    }
}