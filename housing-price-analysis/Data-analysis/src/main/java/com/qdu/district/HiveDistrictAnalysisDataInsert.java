package com.qdu.district;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HiveDistrictAnalysisDataInsert {

    // === Hive 配置 ===
    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz;user=master";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // === MySQL 配置 ===
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://192.168.211.1:3306/cjz?" +
                    "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";       // 👈 替换为实际用户名
    private static final String MYSQL_PASSWORD = "root"; // 👈 替换为实际密码

    // 表配置
    private static final String SOURCE_TABLE = "house_info_clean_checkid";
    private static final String TARGET_TABLE = "district_house_price_analysis";
    private static final String DATABASE = "cjz";

    // 分析参数
    private static int CHECK_ID; // ←←← 修改：移除 final 和初始值
    private static final String PARTITION_VALUE;
    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        PARTITION_VALUE = sdf.format(new Date());
    }

    // UDF JAR 路径
    private static final String UDF_JAR_PATH = "hdfs:///user/master/dataanalysis/DataAnalysis-1.0-SNAPSHOT.jar";

    public static void main(String[] args) {
        // === 新增：从命令行读取 checkid（严格参照 HouseYearAnalysisFinal.java）===
        if (args.length < 1) {
            System.err.println("❌ 错误: 请提供 checkid 参数（例如：java ... com.qdu.district.HiveDistrictAnalysisDataInsert 123）");
            System.exit(1);
        }
        try {
            CHECK_ID = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("❌ 错误: checkid 必须是一个整数，但收到的是: " + args[0]);
            System.exit(1);
        }
        // =====================================

        System.setProperty("HADOOP_USER_NAME", "master");
        System.out.println("===== 参数配置 =====");
        System.out.println("HADOOP_USER_NAME: " + System.getProperty("HADOOP_USER_NAME"));
        System.out.println("分析批次 checkid = " + CHECK_ID);
        System.out.println("分区日期: " + PARTITION_VALUE);
        System.out.println("UDF JAR 路径: " + UDF_JAR_PATH);

        Connection hiveConn = null;
        Connection mysqlConn = null;

        try {
            // === 1. 连接 Hive ===
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            System.out.println("✓ Hive连接成功");

            // >>>>>>>>>>>>>>>>>> 新增：设置 MapReduce 内存参数 <<<<<<<<<<<<<<<<<<
            try (PreparedStatement s1 = hiveConn.prepareStatement("SET mapreduce.map.memory.mb=4096")) { s1.execute(); }
            try (PreparedStatement s2 = hiveConn.prepareStatement("SET mapreduce.reduce.memory.mb=4096")) { s2.execute(); }
            try (PreparedStatement s3 = hiveConn.prepareStatement("SET mapreduce.map.java.opts=-Xmx3276m")) { s3.execute(); }
            try (PreparedStatement s4 = hiveConn.prepareStatement("SET mapreduce.reduce.java.opts=-Xmx3276m")) { s4.execute(); }
            System.out.println("✓ 已设置 MapReduce 内存参数");

            // 注册自定义 UDF（仅房龄）
            registerHouseAgeUDF(hiveConn);

            long sourceCount = checkSourceData(hiveConn);
            if (sourceCount == 0) {
                System.out.println("源表无符合条件数据（checkid=" + CHECK_ID + "），退出");
                return;
            }

            // === 2. 执行 Hive 分析并写入 ===
            insertAnalysisData(hiveConn);
            verifyInsertResult(hiveConn);
            System.out.println("✅ 数据分析与插入完成！");

            // === 3. 连接 MySQL 并同步数据 ===
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("✓ MySQL连接成功");

            syncHiveToMysql(hiveConn, mysqlConn);
            System.out.println("✅ 数据已同步至 MySQL 表 " + TARGET_TABLE);

        } catch (Exception e) {
            System.err.println("❌ 数据处理失败:");
            e.printStackTrace();
        } finally {
            if (hiveConn != null) {
                try {
                    System.out.println("CloseOperation: 关闭 Hive 连接...");
                    hiveConn.close();
                } catch (SQLException ignored) {}
            }
            if (mysqlConn != null) {
                try {
                    System.out.println("CloseOperation: 关闭 MySQL 连接...");
                    mysqlConn.close();
                } catch (SQLException ignored) {}
            }
        }
    }

    /** * 仅注册房龄 UDF */
    private static void registerHouseAgeUDF(Connection conn) throws SQLException {
        System.out.println("\n正在注册房龄 UDF...");
        try (PreparedStatement addJar = conn.prepareStatement("ADD JAR " + UDF_JAR_PATH)) {
            addJar.execute();
            System.out.println("✓ ADD JAR 成功");
        }
        try (PreparedStatement createFunc = conn.prepareStatement(
                "CREATE TEMPORARY FUNCTION calc_house_age AS 'com.qdu.udf.CalculateHouseAgeUDF'")) {
            createFunc.execute();
        }
        System.out.println("✓ 自定义函数注册成功: calc_house_age");
    }

    private static long checkSourceData(Connection conn) throws SQLException {
        String sql = String.format(
                "SELECT COUNT(*) FROM %s.%s WHERE checkid = %d",
                DATABASE, SOURCE_TABLE, CHECK_ID
        );
        try (PreparedStatement stmt = conn.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                long count = rs.getLong(1);
                System.out.println("源表中 checkid=" + CHECK_ID + " 的记录数: " + count);
                return count;
            }
        }
        return 0;
    }

    private static void insertAnalysisData(Connection conn) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO TABLE ").append(DATABASE).append(".").append(TARGET_TABLE)
                .append(" PARTITION (load_date='").append(PARTITION_VALUE).append("')\n")
                .append("SELECT \n")
                .append(" t.district,\n")
                .append(" CAST(ROUND(AVG(t.price_per_sqm)) AS INT) AS avg_price_per_sqm,\n")
                .append(" CAST(COUNT(*) AS INT) AS house_count,\n")
                .append(" MIN(t.price_per_sqm) AS min_price,\n")
                .append(" MAX(t.price_per_sqm) AS max_price,\n")
                .append(" CAST(ROUND(PERCENTILE_APPROX(CAST(t.price_per_sqm AS BIGINT), 0.5)) AS INT) AS median_price,\n")
                .append(" CAST(ROUND(VARIANCE(CAST(t.price_per_sqm AS DOUBLE))) AS INT) AS price_variance,\n")
                .append(" CAST(ROUND(STDDEV(CAST(t.price_per_sqm AS DOUBLE))) AS INT) AS std_price,\n")
                .append(" CAST(ROUND(AVG(calc_house_age(t.build_year))) AS INT) AS avg_house_age,\n")
                .append(" CAST(ROUND(AVG(t.area)) AS INT) AS avg_area,\n")
                .append(" MAX(t.checkid) AS checkid\n")
                .append("FROM (\n")
                .append(" SELECT * FROM ").append(DATABASE).append(".").append(SOURCE_TABLE)
                .append(" WHERE checkid = ").append(CHECK_ID).append("\n")
                .append(" AND district IS NOT NULL AND district != ''\n")
                .append(" AND price_per_sqm > 0 AND price_per_sqm < 500000\n")
                .append(" AND area BETWEEN 10 AND 1000\n")
                .append(" AND build_year RLIKE '^[0-9]{4}$'\n")
                .append(") t\n")
                .append("LEFT SEMI JOIN (\n")
                .append(" SELECT DISTINCT district FROM ").append(DATABASE).append(".").append(SOURCE_TABLE)
                .append(" WHERE checkid = ").append(CHECK_ID).append("\n")
                .append(") d ON t.district = d.district\n")
                .append("GROUP BY t.district\n")
                .append("HAVING COUNT(*) >= 5 AND AVG(t.price_per_sqm) > 10000\n")
                .append("ORDER BY avg_price_per_sqm DESC");

        System.out.println("\n执行分析SQL（含自定义 UDF + 内置函数）:");
        printLine(120);
        System.out.println(sql.toString());
        printLine(120);

        long start = System.currentTimeMillis();
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.executeUpdate();
            long end = System.currentTimeMillis();
            System.out.println("✓ 插入成功，耗时: " + (end - start) + " ms");
            System.out.println("✓ 数据写入分区: load_date='" + PARTITION_VALUE + "', checkid=" + CHECK_ID);
        }
    }

    private static void verifyInsertResult(Connection conn) throws SQLException {
        String verifySQL = String.format(
                "SELECT COUNT(*) cnt, SUM(house_count) total_houses " +
                        "FROM %s.%s WHERE load_date='%s' AND checkid=%d",
                DATABASE, TARGET_TABLE, PARTITION_VALUE, CHECK_ID
        );
        try (PreparedStatement stmt = conn.prepareStatement(verifySQL); ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                long cnt = rs.getLong("cnt");
                long total = rs.getLong("total_houses");
                System.out.println("\n验证结果:");
                System.out.println(" 插入市区数: " + cnt);
                System.out.println(" 总房屋数: " + total);
            }
        }
    }

    // ================= 新增方法：同步 Hive → MySQL =================
    private static void syncHiveToMysql(Connection hiveConn, Connection mysqlConn) throws SQLException {
        // === 先删除 MySQL 中当前 checkid 的所有旧数据 ===
        String deleteSql = "DELETE FROM district_house_price_analysis WHERE checkid = ?";
        try (PreparedStatement delStmt = mysqlConn.prepareStatement(deleteSql)) {
            delStmt.setInt(1, CHECK_ID);
            int deleted = delStmt.executeUpdate();
            System.out.println("🗑️ 已删除 MySQL 中 checkid=" + CHECK_ID + " 的旧记录数: " + deleted);
        }

        // === 从 Hive 读取当前分区和批次的数据 ===
        String selectSql = String.format(
                "SELECT " +
                        "district, avg_price_per_sqm, house_count, min_price, max_price, " +
                        "median_price, price_variance, std_price, avg_house_age, avg_area, " +
                        "checkid " +
                        "FROM %s.%s " +
                        "WHERE load_date = '%s' AND checkid = %d",
                DATABASE, TARGET_TABLE,
                PARTITION_VALUE, CHECK_ID
        );

        System.out.println("🔄 正在从 Hive 读取数据用于同步到 MySQL...");
        List<Object[]> rows = new ArrayList<>();
        try (PreparedStatement hiveStmt = hiveConn.prepareStatement(selectSql);
             ResultSet rs = hiveStmt.executeQuery()) {

            while (rs.next()) {
                Object[] row = {
                        rs.getString("district"),
                        rs.getObject("avg_price_per_sqm"),
                        rs.getObject("house_count"),
                        rs.getObject("min_price"),
                        rs.getObject("max_price"),
                        rs.getObject("median_price"),
                        rs.getObject("price_variance"),
                        rs.getObject("std_price"),
                        rs.getObject("avg_house_age"),
                        rs.getObject("avg_area"),
                        rs.getInt("checkid"),
                        PARTITION_VALUE // load_date 作为普通字段插入
                };
                rows.add(row);
            }
        }

        if (rows.isEmpty()) {
            System.out.println("⚠️ Hive 中未找到待同步数据（load_date='" + PARTITION_VALUE + "', checkid=" + CHECK_ID + "）");
            return;
        }

        // === 批量插入到 MySQL ===
        String insertSql = "INSERT INTO district_house_price_analysis (" +
                "district, avg_price_per_sqm, house_count, min_price, max_price, " +
                "median_price, price_variance, std_price, avg_house_age, avg_area, " +
                "checkid, load_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement mysqlStmt = mysqlConn.prepareStatement(insertSql)) {
            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    mysqlStmt.setObject(i + 1, row[i]);
                }
                mysqlStmt.addBatch();
            }
            int[] results = mysqlStmt.executeBatch();
            System.out.println("✅ 成功写入 MySQL " + results.length + " 条记录");
        }
    }

    private static void printLine(int n) {
        int len = Math.min(n, 200);
        for (int i = 0; i < len; i++) {
            System.out.print('=');
        }
        System.out.println();
    }
}