package com.qdu.led;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import java.sql.*;

public class HouseAnalysisJob {

    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz;user=master";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // === 新增：MySQL 配置 ===
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://192.168.211.1:3306/cjz?" +
                    "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";       // 👈 替换为实际用户名
    private static final String MYSQL_PASSWORD = "root"; // 👈 替换为实际密码

    // 改为从命令行接收 checkid
    private static int CHECK_ID;

    public static void main(String[] args) {
        // === 从命令行读取 checkid（参考 HouseYearAnalysisFinal.java）===
        if (args.length < 1) {
            System.err.println("❌ 错误: 请提供 checkid 参数（例如：java ... com.qdu.led.HouseAnalysisJob 123）");
            System.exit(1);
        }
        try {
            CHECK_ID = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("❌ 错误: checkid 必须是一个整数，但收到的是: " + args[0]);
            System.exit(1);
        }
        // =====================================

        System.out.println("开始执行房屋数据分析任务（checkid=" + CHECK_ID + "）...");
        Connection hiveConn = null;
        Connection mysqlConn = null;
        Statement stmt = null;

        try {
            // === 1. 连接 Hive 并执行分析 ===
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            stmt = hiveConn.createStatement();

            // >>>>>>>>>>>>>>>>>> 新增：设置 MapReduce 内存参数 <<<<<<<<<<<<<<<<<<
            stmt.execute("SET mapreduce.map.memory.mb=4096");
            stmt.execute("SET mapreduce.reduce.memory.mb=4096");
            stmt.execute("SET mapreduce.map.java.opts=-Xmx3276m");
            stmt.execute("SET mapreduce.reduce.java.opts=-Xmx3276m");
            System.out.println("✓ 已设置 MapReduce 内存参数");

            // 注册临时函数
            stmt.execute("ADD JAR hdfs:///user/master/dataanalysis/DataAnalysis-1.0-SNAPSHOT.jar");
            stmt.execute("CREATE TEMPORARY FUNCTION classify_layout AS 'com.qdu.udf.LayoutClassifyUDF'");
            stmt.execute("CREATE TEMPORARY FUNCTION classify_decoration AS 'com.qdu.udf.DecorationClassifyUDF'");

            // 启用分桶写入
            stmt.execute("SET hive.enforce.bucketing = true");
            stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");

            // 构建 HQL
            String insertHql = "INSERT INTO TABLE house_analysis_result " +
                    "SELECT " +
                    " layout_cat, " +
                    " elevator_int, " +
                    " deco_cat, " +
                    " COUNT(*) AS house_count, " +
                    " CAST(AVG(price_per_sqm) AS INT) AS avg_price_per_sqm, " +
                    " " + CHECK_ID + " AS checkid " +
                    "FROM ( " +
                    " SELECT " +
                    " classify_layout(layout) AS layout_cat, " +
                    " elevator_int, " +
                    " classify_decoration(decoration) AS deco_cat, " +
                    " price_per_sqm " +
                    " FROM house_info_clean_checkid " +
                    " WHERE checkid = " + CHECK_ID + " " +
                    " AND layout IS NOT NULL " +
                    " AND decoration IS NOT NULL " +
                    " AND price_per_sqm IS NOT NULL " +
                    " AND elevator_int IN (0, 1) " +
                    ") t " +
                    "GROUP BY layout_cat, elevator_int, deco_cat " +
                    "ORDER BY layout_cat, elevator_int, deco_cat";

            System.out.println("正在执行分析与插入...");
            int rows = stmt.executeUpdate(insertHql);
            System.out.println("HQL 执行完成（Hive 不返回实际插入行数，rows=" + rows + "）");

            // 验证结果
            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) AS total FROM house_analysis_result WHERE checkid = " + CHECK_ID
            );
            if (rs.next()) {
                int count = rs.getInt("total");
                System.out.println("验证：house_analysis_result 中 checkid=" + CHECK_ID + " 的记录数 = " + count);
                if (count == 24) {
                    System.out.println("✓ 符合预期：3 × 2 × 4 = 24");
                } else {
                    System.out.println("⚠ 实际组合数：" + count + "（某些分类无数据）");
                }
            }
            rs.close();

            // === 2. 同步数据到 MySQL ===
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("✓ 成功连接 MySQL");

            syncHiveToMysql(hiveConn, mysqlConn);

            System.out.println("✅ 数据已同步至 MySQL 表 house_analysis_result");

        } catch (Exception e) {
            System.err.println("执行失败：");
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (hiveConn != null) hiveConn.close();
                if (mysqlConn != null) mysqlConn.close();
            } catch (SQLException ignored) {}
        }
    }

    // ================= 新增方法：同步 Hive → MySQL =================
    private static void syncHiveToMysql(Connection hiveConn, Connection mysqlConn) throws SQLException {
        // 1. 删除 MySQL 中当前 checkid 的旧数据（确保幂等）
        String deleteSql = "DELETE FROM house_analysis_result WHERE checkid = ?";
        try (PreparedStatement delStmt = mysqlConn.prepareStatement(deleteSql)) {
            delStmt.setInt(1, CHECK_ID);
            int deleted = delStmt.executeUpdate();
            System.out.println("🗑️ 已删除 MySQL 中 checkid=" + CHECK_ID + " 的旧记录数: " + deleted);
        }

        // 2. 从 Hive 读取当前批次数据
        String selectSql = "SELECT layout_category, elevator_int, decoration_category, " +
                "house_count, avg_price_per_sqm, checkid " +
                "FROM house_analysis_result " +
                "WHERE checkid = " + CHECK_ID;

        System.out.println("🔄 正在从 Hive 读取 checkid=" + CHECK_ID + " 的分析结果...");

        PreparedStatement hiveStmt = hiveConn.prepareStatement(selectSql);
        ResultSet rs = hiveStmt.executeQuery();

        // 3. 批量插入到 MySQL
        String insertSql = "INSERT INTO house_analysis_result (" +
                "layout_category, elevator_int, decoration_category, " +
                "house_count, avg_price_per_sqm, checkid) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        PreparedStatement mysqlStmt = mysqlConn.prepareStatement(insertSql);
        int count = 0;
        while (rs.next()) {
            mysqlStmt.setString(1, rs.getString("layout_category"));
            mysqlStmt.setObject(2, rs.getObject("elevator_int")); // 可为 null
            mysqlStmt.setString(3, rs.getString("decoration_category"));
            mysqlStmt.setInt(4, rs.getInt("house_count"));
            mysqlStmt.setInt(5, rs.getInt("avg_price_per_sqm"));
            mysqlStmt.setInt(6, rs.getInt("checkid"));
            mysqlStmt.addBatch();
            count++;
        }

        if (count > 0) {
            int[] results = mysqlStmt.executeBatch();
            System.out.println("✅ 成功写入 MySQL " + results.length + " 条记录");
        } else {
            System.out.println("⚠️ Hive 中未找到 checkid=" + CHECK_ID + " 的数据，跳过同步");
        }

        rs.close();
        hiveStmt.close();
        mysqlStmt.close();
    }
}