package com.qdu.value;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 性价比房屋分析任务
 * - 使用 CROSS JOIN 生成所有组合
 * - 调用 UDF 计算加权单价和性价比评分
 * - 写入分区表 best_value_housing
 * - 新增：同步结果到 MySQL 表 best_value_housing
 */
public class BestValueHousingJob {

    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz;user=master";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // === 新增：MySQL 配置 ===
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://192.168.211.1:3306/cjz?" +
                    "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";       // 👈 替换为实际用户名
    private static final String MYSQL_PASSWORD = "root"; // 👈 替换为实际密码

    // 改为从命令行参数接收 checkid
    private static int CHECK_ID;

    private static final String UDF_JAR_PATH = "hdfs:///user/master/dataanalysis/DataAnalysis-1.0-SNAPSHOT.jar";

    // 获取当前日期字符串（格式：yyyy-MM-dd），与 Hive 的 CURRENT_DATE() 一致
    private static final String PT_DATE = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

    public static void main(String[] args) {
        // 从命令行接收 checkid
        if (args.length < 1) {
            System.err.println("❌ 错误: 请提供 checkid 参数（例如：java ... com.qdu.value.BestValueHousingJob 123）");
            System.exit(1);
        }
        try {
            CHECK_ID = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("❌ 错误: checkid 必须是一个整数，但收到的是: " + args[0]);
            System.exit(1);
        }

        System.out.println("🚀 开始执行性价比房屋分析任务（checkid=" + CHECK_ID + ", pt_date=" + PT_DATE + "）...");

        Connection hiveConn = null;
        Connection mysqlConn = null;
        Statement stmt = null;

        try {
            // === 1. 执行 Hive 分析与写入 ===
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            stmt = hiveConn.createStatement();

            // 设置 Hive 参数
            stmt.execute("SET hive.exec.dynamic.partition = true");
            stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
            stmt.execute("SET hive.strict.checks.cartesian.product = false");
            stmt.execute("SET hive.mapred.mode = nonstrict");

            // 注册 UDF
            stmt.execute("ADD JAR " + UDF_JAR_PATH);
            stmt.execute("CREATE TEMPORARY FUNCTION weighted_price AS 'com.qdu.udf.WeightedPriceUDF'");
            stmt.execute("CREATE TEMPORARY FUNCTION value_score_udf AS 'com.qdu.udf.ValueScoreUDF'");

            // 执行插入（无 GROUP BY！）
            String sql = "INSERT INTO TABLE best_value_housing PARTITION (pt_date) " +
                    "SELECT " +
                    " d.district, " +
                    " a.area_range, " +
                    " h.layout_category, " +
                    " h.decoration_category, " +
                    " h.elevator_int, " +
                    " weighted_price(d.avg_price_per_sqm, a.avg_price_per_sqm, h.avg_price_per_sqm), " +
                    " value_score_udf(d.district, a.area_range, h.layout_category, h.decoration_category, h.elevator_int), " +
                    " " + CHECK_ID + ", " +
                    " CURRENT_DATE() " +
                    "FROM " +
                    " (SELECT * FROM house_analysis_result WHERE checkid = " + CHECK_ID + ") h " +
                    "CROSS JOIN " +
                    " (SELECT * FROM area_price_analysis WHERE checkid = " + CHECK_ID + ") a " +
                    "CROSS JOIN " +
                    " (SELECT * FROM district_house_price_analysis WHERE checkid = " + CHECK_ID + ") d " +
                    "WHERE " +
                    " d.avg_price_per_sqm IS NOT NULL " +
                    " AND a.avg_price_per_sqm IS NOT NULL " +
                    " AND h.avg_price_per_sqm IS NOT NULL";

            System.out.println("正在执行 Hive 插入...");
            stmt.executeUpdate(sql);
            System.out.println("✅ Hive 任务成功完成！数据已写入 best_value_housing 表。");

            // === 2. 同步数据到 MySQL ===
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("✓ 成功连接 MySQL");

            syncHiveToMysql(hiveConn, mysqlConn);

            System.out.println("✅ 数据已同步至 MySQL 表 best_value_housing");

        } catch (Exception e) {
            System.err.println("❌ 任务失败：");
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (hiveConn != null) hiveConn.close();
                if (mysqlConn != null) mysqlConn.close();
            } catch (Exception ignored) {}
        }
    }

    // ================= 新增方法：同步 Hive → MySQL =================
    private static void syncHiveToMysql(Connection hiveConn, Connection mysqlConn) throws Exception {
        // 1. 删除 MySQL 中当前 (checkid, pt_date) 的旧数据（确保幂等）
        String deleteSql = "DELETE FROM best_value_housing WHERE checkid = ? AND pt_date = ?";
        try (PreparedStatement delStmt = mysqlConn.prepareStatement(deleteSql)) {
            delStmt.setInt(1, CHECK_ID);
            delStmt.setString(2, PT_DATE);
            int deleted = delStmt.executeUpdate();
            System.out.println("🗑️ 已删除 MySQL 中 checkid=" + CHECK_ID + ", pt_date='" + PT_DATE + "' 的旧记录数: " + deleted);
        }

        // 2. 从 Hive 读取当前批次 + 分区的数据
        String selectSql = "SELECT " +
                "district, area_range, layout_category, decoration_category, " +
                "elevator_int, avg_price_per_sqm, value_score, checkid, pt_date " +
                "FROM best_value_housing " +
                "WHERE checkid = " + CHECK_ID + " AND pt_date = '" + PT_DATE + "'";

        System.out.println("🔄 正在从 Hive 读取 checkid=" + CHECK_ID + ", pt_date='" + PT_DATE + "' 的数据...");

        PreparedStatement hiveStmt = hiveConn.prepareStatement(selectSql);
        ResultSet rs = hiveStmt.executeQuery();

        // 3. 批量插入到 MySQL
        String insertSql = "INSERT INTO best_value_housing (" +
                "district, area_range, layout_category, decoration_category, " +
                "elevator_int, avg_price_per_sqm, value_score, checkid, pt_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        PreparedStatement mysqlStmt = mysqlConn.prepareStatement(insertSql);
        int count = 0;
        while (rs.next()) {
            mysqlStmt.setString(1, rs.getString("district"));
            mysqlStmt.setString(2, rs.getString("area_range"));
            mysqlStmt.setString(3, rs.getString("layout_category"));
            mysqlStmt.setString(4, rs.getString("decoration_category"));
            mysqlStmt.setObject(5, rs.getObject("elevator_int")); // 可为 null
            mysqlStmt.setObject(6, rs.getObject("avg_price_per_sqm"));
            mysqlStmt.setObject(7, rs.getObject("value_score"));
            mysqlStmt.setInt(8, rs.getInt("checkid"));
            mysqlStmt.setString(9, rs.getString("pt_date"));
            mysqlStmt.addBatch();
            count++;
        }

        if (count > 0) {
            int[] results = mysqlStmt.executeBatch();
            System.out.println("✅ 成功写入 MySQL " + results.length + " 条记录");
        } else {
            System.out.println("⚠️ Hive 中未找到匹配数据，跳过同步");
        }

        rs.close();
        hiveStmt.close();
        mysqlStmt.close();
    }
}