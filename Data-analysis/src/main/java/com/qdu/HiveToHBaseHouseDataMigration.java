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
    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";
    private static final String ZOOKEEPER_QUORUM = "hadoop101";

    // HBase表和列族配置（更新为新表名）
    private static final String NAMESPACE = "cjz";
    private static final String TABLE_NAME = "house_info_clean_checkid"; // ← 修改表名
    private static final String COLUMN_FAMILY = "info";

    public static void main(String[] args) {
        try {
            System.out.println("开始房屋数据迁移任务（含 checkid）...");
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

    /** * 创建HBase表 */
    private static void createHBaseTable() throws IOException {
        System.out.println("开始创建HBase表...");
        Configuration hbaseConf = getHBaseConfiguration();
        org.apache.hadoop.hbase.client.Connection hbaseConn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf);
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

            TableName tableName = TableName.valueOf(NAMESPACE + ":" + TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                cfBuilder.setMaxVersions(1);
                tableBuilder.setColumnFamily(cfBuilder.build());
                admin.createTable(tableBuilder.build());
                System.out.println("表 " + NAMESPACE + ":" + TABLE_NAME + " 创建成功");
            } else {
                System.out.println("表 " + NAMESPACE + ":" + TABLE_NAME + " 已存在");
            }
        } finally {
            if (admin != null) admin.close();
            if (hbaseConn != null) hbaseConn.close();
        }
    }

    /** * 从Hive迁移数据到HBase（使用新表 house_info_clean_checkid） */
    private static void migrateDataFromHiveToHBase() throws Exception {
        System.out.println("开始从Hive迁移数据到HBase（含 checkid 字段）...");

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        java.sql.Connection hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);

        // 查询新表：包含 checkid
        String query = "SELECT " +
                "rowkey, district, community, layout, orientation, " +
                "floor_num, decoration, elevator_int, area, price, " +
                "price_per_sqm, build_year, house_age, checkid " + // ← 新增 checkid
                "FROM house_info_clean_checkid"; // ← 表名变更

        PreparedStatement stmt = hiveConn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();

        Configuration hbaseConf = getHBaseConfiguration();
        org.apache.hadoop.hbase.client.Connection hbaseConn = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf);
        Table table = hbaseConn.getTable(TableName.valueOf(NAMESPACE + ":" + TABLE_NAME));

        try {
            int count = 0;
            int batchSize = 100;
            java.util.List<Put> puts = new java.util.ArrayList<>();

            while (rs.next()) {
                count++;
                String rowkey = rs.getString("rowkey");
                Put put = new Put(Bytes.toBytes(rowkey));

                // 原有字段
                addColumnIfNotNull(put, "district", rs.getString("district"));
                addColumnIfNotNull(put, "community", rs.getString("community"));
                addColumnIfNotNull(put, "layout", rs.getString("layout"));
                addColumnIfNotNull(put, "orientation", rs.getString("orientation"));
                addColumnIfNotNull(put, "decoration", rs.getString("decoration"));
                addColumnIfNotNull(put, "build_year", rs.getString("build_year"));

                addIntColumnIfNotNull(put, "floor_num", rs.getInt("floor_num"));
                addIntColumnIfNotNull(put, "elevator_int", rs.getInt("elevator_int"));
                addIntColumnIfNotNull(put, "area", rs.getInt("area"));
                addIntColumnIfNotNull(put, "price", rs.getInt("price"));
                addIntColumnIfNotNull(put, "price_per_sqm", rs.getInt("price_per_sqm"));
                addIntColumnIfNotNull(put, "house_age", rs.getInt("house_age"));

                // 新增字段：checkid（整数）
                addIntColumnIfNotNull(put, "checkid", rs.getInt("checkid")); // ← 新增

                puts.add(put);

                if (puts.size() >= batchSize) {
                    table.put(puts);
                    System.out.println("已处理 " + count + " 条记录");
                    puts.clear();
                }
                if (count % 1000 == 0) {
                    System.out.println("处理进度: " + count + " 条记录");
                }
            }

            if (!puts.isEmpty()) {
                table.put(puts);
                System.out.println("提交最后一批数据，共 " + puts.size() + " 条");
            }
            System.out.println("总共迁移了 " + count + " 条记录");

            verifyDataIntegrity(count, hiveConn, hbaseConn);
        } finally {
            if (table != null) table.close();
            if (hbaseConn != null) hbaseConn.close();
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (hiveConn != null) hiveConn.close();
        }
    }

    private static void addColumnIfNotNull(Put put, String column, String value) {
        if (value != null && !value.trim().isEmpty()) {
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes(value));
        }
    }

    private static void addIntColumnIfNotNull(Put put, String column, int value) {
        // 注意：即使值为0（如 checkid=0），也应写入，因为0是有效值
        // 所以这里不再判断是否为“空”，而是直接写入（Hive中 INT NULL 会返回 0，需注意）
        // 如果需要区分 NULL，建议用 getObject 并判断 null，但为简化，按当前逻辑处理
        put.addColumn(
                Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(column),
                Bytes.toBytes(String.valueOf(value))
        );
    }

    /** * 验证数据完整性（更新抽样查询字段） */
    private static void verifyDataIntegrity(int expectedCount, java.sql.Connection hiveConn, org.apache.hadoop.hbase.client.Connection hbaseConn) throws Exception {
        System.out.println("开始验证数据完整性...");
        Table table = null;
        try {
            table = hbaseConn.getTable(TableName.valueOf(NAMESPACE + ":" + TABLE_NAME));

            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner scanner = table.getScanner(scan);
            int hbaseCount = 0;
            for (Result result : scanner) hbaseCount++;
            scanner.close();

            System.out.println("HBase中实际记录数: " + hbaseCount);
            System.out.println("Hive中预期记录数: " + expectedCount);
            if (hbaseCount == expectedCount) {
                System.out.println("✓ 数据完整性验证通过！");
            } else {
                System.out.println("⚠ 警告: 数据数量不匹配！");
            }

            // 抽样验证增加 checkid
            System.out.println("正在进行随机抽样验证（含 checkid）...");
            String sampleQuery = "SELECT rowkey, district, price, checkid FROM house_info_clean_checkid LIMIT 5";
            PreparedStatement sampleStmt = hiveConn.prepareStatement(sampleQuery);
            ResultSet sampleRs = sampleStmt.executeQuery();
            while (sampleRs.next()) {
                String rowkey = sampleRs.getString("rowkey");
                String expectedDistrict = sampleRs.getString("district");
                String expectedPrice = String.valueOf(sampleRs.getInt("price"));
                String expectedCheckid = String.valueOf(sampleRs.getInt("checkid"));

                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    String actualDistrict = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("district")));
                    String actualPrice = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("price")));
                    String actualCheckid = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("checkid")));

                    if (expectedDistrict != null && expectedDistrict.equals(actualDistrict)
                            && expectedPrice.equals(actualPrice)
                            && expectedCheckid.equals(actualCheckid)) {
                        System.out.println("✓ 抽样验证通过 - RowKey: " + rowkey);
                    } else {
                        System.out.println("✗ 抽样验证失败 - RowKey: " + rowkey);
                        System.out.println(" 期望: district=" + expectedDistrict + ", price=" + expectedPrice + ", checkid=" + expectedCheckid);
                        System.out.println(" 实际: district=" + actualDistrict + ", price=" + actualPrice + ", checkid=" + actualCheckid);
                    }
                } else {
                    System.out.println("✗ 数据未找到 - RowKey: " + rowkey);
                }
            }
            sampleRs.close();
            sampleStmt.close();
        } finally {
            if (table != null) table.close();
        }
    }

    private static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        conf.setInt("hbase.rpc.timeout", 60000);
        conf.setInt("hbase.client.operation.timeout", 60000);
        conf.setInt("hbase.client.scanner.timeout.period", 60000);
        conf.setInt("hbase.client.write.buffer", 2097152);
        conf.setBoolean("hbase.client.enable.automatic.flush", true);
        conf.setLong("hbase.client.flush.interval", 1000);
        return conf;
    }
}