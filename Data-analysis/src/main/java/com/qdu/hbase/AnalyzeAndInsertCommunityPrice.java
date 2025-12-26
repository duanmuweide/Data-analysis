package com.qdu.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class AnalyzeAndInsertCommunityPrice {

    private static final String SOURCE_TABLE_NAME = "cjz:house_info_clean_checkid";
    private static final String TARGET_HBASE_TABLE_NAME = "cjz:community_price_analysis";
    private static final int TARGET_CHECKID = 3;

    // MySQL 配置
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://localhost:3306/cjz?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // HBase 连接（显式类型）
    private static org.apache.hadoop.hbase.client.Connection hbaseConnection = null;
    private static Table sourceTable = null;
    private static Table targetTable = null;

    public static void main(String[] args) {
        System.out.println("===== 小区房价分析程序启动 =====");
        System.out.println("源表: " + SOURCE_TABLE_NAME);
        System.out.println("目标HBase表: " + TARGET_HBASE_TABLE_NAME);
        System.out.println("分析批次 checkid = " + TARGET_CHECKID);
        System.out.println("================================");

        try {
            initHBase();
            analyzeAndInsert();
            System.out.println("\n✅ HBase 写入完成！");
        } catch (Exception e) {
            System.err.println("❌ 程序执行失败:");
            e.printStackTrace();
        } finally {
            closeHBase();
        }
    }

    private static void initHBase() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        hbaseConnection = ConnectionFactory.createConnection(conf);
        sourceTable = hbaseConnection.getTable(TableName.valueOf(SOURCE_TABLE_NAME));
        targetTable = hbaseConnection.getTable(TableName.valueOf(TARGET_HBASE_TABLE_NAME));

        System.out.println("✓ HBase 连接初始化成功");
    }

    private static void closeHBase() {
        try {
            if (sourceTable != null) sourceTable.close();
            if (targetTable != null) targetTable.close();
            if (hbaseConnection != null) hbaseConnection.close();
        } catch (IOException ignored) {}
    }

    private static void analyzeAndInsert() throws IOException {
        // ✅ 正确创建 SingleColumnValueFilter（HBase 2.x+）
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("checkid"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(String.valueOf(TARGET_CHECKID))
        );
        filter.setFilterIfMissing(true); // 如果列不存在，过滤掉该行

        Scan scan = new Scan();
        scan.setFilter(filter); // ✅ 只传一个 Filter 对象

        ResultScanner scanner = sourceTable.getScanner(scan);
        Map<String, CommunityStats> communityMap = new HashMap<>();

        System.out.println("正在扫描源表数据...");

        int processed = 0;
        for (Result result : scanner) {
            processed++;
            if (processed % 10000 == 0) {
                System.out.println("已处理 " + processed + " 行...");
            }

            String district = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("district")));
            String community = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("community")));
            String priceStr = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("price_per_sqm")));
            String buildYear = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("build_year")));

            if (district == null || community == null || priceStr == null) continue;

            try {
                int price = Integer.parseInt(priceStr.trim());
                String key = district + "||" + community;
                communityMap.computeIfAbsent(key, k -> new CommunityStats(district, community, buildYear))
                        .addPrice(price);
            } catch (NumberFormatException ignored) {}
        }
        scanner.close();

        System.out.println("共聚合 " + communityMap.size() + " 个小区的数据。");

        // 写入 HBase
        List<Put> puts = new ArrayList<>();
        for (CommunityStats stats : communityMap.values()) {
            String rowKey = TARGET_CHECKID + "_" + stats.district + "_" + stats.community;
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("district"), Bytes.toBytes(stats.district));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("community"), Bytes.toBytes(stats.community));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("house_count"), Bytes.toBytes(String.valueOf(stats.count)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("avg_price_per_sqm"), Bytes.toBytes(String.valueOf(stats.avgPrice())));
            if (stats.buildYear != null && !stats.buildYear.isEmpty()) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("build_year"), Bytes.toBytes(stats.buildYear));
            }
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("checkid"), Bytes.toBytes(String.valueOf(TARGET_CHECKID)));
            puts.add(put);
        }

        targetTable.put(puts);
        System.out.println("✅ 成功写入 " + puts.size() + " 条记录到 HBase 表 " + TARGET_HBASE_TABLE_NAME);

        // 同步到 MySQL
        syncToMysql(communityMap, TARGET_CHECKID);
    }

    private static void syncToMysql(Map<String, CommunityStats> communityMap, int checkid) {
        java.sql.Connection conn = null; // ✅ 显式使用 java.sql.Connection
        java.sql.PreparedStatement deleteStmt = null;
        java.sql.PreparedStatement insertStmt = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            conn.setAutoCommit(false);

            // 删除旧数据
            String deleteSql = "DELETE FROM community_price_analysis WHERE checkid = ?";
            deleteStmt = conn.prepareStatement(deleteSql);
            deleteStmt.setInt(1, checkid);
            int deleted = deleteStmt.executeUpdate();
            System.out.println("🗑️ 已删除 MySQL 中 checkid=" + checkid + " 的旧记录数: " + deleted);

            // 批量插入
            String insertSql = "INSERT INTO community_price_analysis " +
                    "(checkid, district, community, house_count, avg_price_per_sqm, build_year) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
            insertStmt = conn.prepareStatement(insertSql);

            int batchCount = 0;
            for (CommunityStats stats : communityMap.values()) {
                insertStmt.setInt(1, checkid);
                insertStmt.setString(2, stats.district);
                insertStmt.setString(3, stats.community);
                insertStmt.setInt(4, stats.count);
                insertStmt.setInt(5, stats.avgPrice());
                insertStmt.setString(6, stats.buildYear == null ? "" : stats.buildYear);
                insertStmt.addBatch();
                batchCount++;

                if (batchCount % 1000 == 0) {
                    insertStmt.executeBatch();
                    System.out.println("✅ 已同步 " + batchCount + " 条记录到 MySQL...");
                }
            }

            if (batchCount > 0 && batchCount % 1000 != 0) {
                insertStmt.executeBatch();
            }

            conn.commit();
            System.out.println("✅ 成功同步 " + batchCount + " 条记录到 MySQL 表 community_price_analysis");

        } catch (Exception e) {
            System.err.println("❌ 同步到 MySQL 失败:");
            e.printStackTrace();
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException rollbackEx) {
                    rollbackEx.printStackTrace();
                }
            }
        } finally {
            try {
                if (deleteStmt != null) deleteStmt.close();
                if (insertStmt != null) insertStmt.close();
                if (conn != null) conn.close();
            } catch (SQLException ignored) {}
        }
    }

    private static class CommunityStats {
        String district;
        String community;
        String buildYear;
        int count = 0;
        long totalPrice = 0;

        public CommunityStats(String district, String community, String buildYear) {
            this.district = district;
            this.community = community;
            this.buildYear = buildYear;
        }

        public void addPrice(int price) {
            count++;
            totalPrice += price;
        }

        public int avgPrice() {
            return count > 0 ? (int) (totalPrice / count) : 0;
        }
    }
}