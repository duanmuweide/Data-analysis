package com.qdu.year;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 房屋建造年份与户型、电梯、装修情况关联分析
 * 专门分析checkid=ANALYSIS_CHECK_ID的数据，并插入到house_year_analysis表
 * 适配Hive 2.3.7版本（不支持CTE）
 * 新增：同步结果到 MySQL 表 house_year_analysis
 */
public class HouseYearAnalysisFinal {

    // Hive连接参数
    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz;user=master";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // === 新增：MySQL 配置 ===
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://192.168.211.1:3306/cjz?" +
                    "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";       // 👈 替换为实际用户名
    private static final String MYSQL_PASSWORD = "root"; // 👈 替换为实际密码

    // 表配置
    private static final String SOURCE_TABLE = "house_info_clean_checkid";
    private static final String TARGET_TABLE = "house_year_analysis";
    private static final String DATABASE = "cjz";

    // ============ 配置参数：改为从命令行接收 ============
    private static int ANALYSIS_CHECK_ID; // 移除 final 和初始值
    // =============================================

    // 分区日期（格式：yyyyMMdd，如 20251225）
    private static final String PT_DATE;
    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        PT_DATE = sdf.format(new Date());
    }

    public static void main(String[] args) {
        // === 新增：从命令行读取 checkid ===
        if (args.length < 1) {
            System.err.println("❌ 错误: 请提供 checkid 参数（例如：java ... com.qdu.year.HouseYearAnalysisFinal 123）");
            System.exit(1);
        }
        try {
            ANALYSIS_CHECK_ID = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("❌ 错误: checkid 必须是一个整数，但收到的是: " + args[0]);
            System.exit(1);
        }
        // =====================================

        // 设置Hadoop用户身份
        System.setProperty("HADOOP_USER_NAME", "master");

        System.out.println("===== 房屋年份复杂分析程序开始 =====");
        System.out.println("源表: " + SOURCE_TABLE);
        System.out.println("目标表: " + TARGET_TABLE);
        System.out.println("分析批次: checkid = " + ANALYSIS_CHECK_ID);
        System.out.println("分区日期: " + PT_DATE);
        System.out.println("=====================================");

        Connection hiveConn = null;
        Connection mysqlConn = null;
        try {
            // 1. 建立Hive连接
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            System.out.println("✓ Hive连接成功");

            // 2. 设置Hive参数
            setupHiveParameters(hiveConn);

            // 3. 检查源表数据
            checkSourceData(hiveConn);

            // 4. 执行复杂的数据分析并插入数据
            executeComplexAnalysis(hiveConn);

            // 5. 验证插入结果
            verifyResults(hiveConn);

            System.out.println("\n✅ 房屋年份分析完成！数据已插入到表: " + TARGET_TABLE);

            // === 6. 新增：同步到 MySQL ===
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("✓ 成功连接 MySQL");

            syncHiveToMysql(hiveConn, mysqlConn);

            System.out.println("✅ 数据已同步至 MySQL 表 house_year_analysis");

        } catch (Exception e) {
            System.err.println("❌ 程序执行出错:");
            e.printStackTrace();
        } finally {
            if (hiveConn != null) {
                try { hiveConn.close(); } catch (SQLException ignored) {}
            }
            if (mysqlConn != null) {
                try { mysqlConn.close(); } catch (SQLException ignored) {}
            }
        }
    }

    // ================= 新增方法：同步 Hive → MySQL =================
    private static void syncHiveToMysql(Connection hiveConn, Connection mysqlConn) throws SQLException {
        // 1. 删除 MySQL 中当前 (checkid, pt_date) 的旧数据（确保幂等）
        String deleteSql = "DELETE FROM house_year_analysis WHERE checkid = ? AND pt_date = ?";
        try (PreparedStatement delStmt = mysqlConn.prepareStatement(deleteSql)) {
            delStmt.setInt(1, ANALYSIS_CHECK_ID);
            delStmt.setString(2, PT_DATE);
            int deleted = delStmt.executeUpdate();
            System.out.println("🗑️ 已删除 MySQL 中 checkid=" + ANALYSIS_CHECK_ID + ", pt_date='" + PT_DATE + "' 的旧记录数: " + deleted);
        }

        // 2. 从 Hive 读取当前批次 + 分区的数据
        String selectSql = "SELECT " +
                "year_range, house_count, elevator_count, small_layout_count, medium_layout_count, large_layout_count, " +
                "premium_decoration_count, simple_decoration_count, rough_decoration_count, analysis_time, checkid " +
                "FROM " + DATABASE + "." + TARGET_TABLE + " " +
                "WHERE checkid = " + ANALYSIS_CHECK_ID + " AND pt_date = '" + PT_DATE + "'";

        System.out.println("🔄 正在从 Hive 读取 checkid=" + ANALYSIS_CHECK_ID + ", pt_date='" + PT_DATE + "' 的数据...");

        PreparedStatement hiveStmt = hiveConn.prepareStatement(selectSql);
        ResultSet rs = hiveStmt.executeQuery();

        // 3. 批量插入到 MySQL
        String insertSql = "INSERT INTO house_year_analysis (" +
                "year_range, house_count, elevator_count, small_layout_count, medium_layout_count, large_layout_count, " +
                "premium_decoration_count, simple_decoration_count, rough_decoration_count, analysis_time, checkid, pt_date" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        PreparedStatement mysqlStmt = mysqlConn.prepareStatement(insertSql);
        int count = 0;
        while (rs.next()) {
            mysqlStmt.setString(1, rs.getString("year_range"));
            mysqlStmt.setInt(2, rs.getInt("house_count"));
            mysqlStmt.setInt(3, rs.getInt("elevator_count"));
            mysqlStmt.setInt(4, rs.getInt("small_layout_count"));
            mysqlStmt.setInt(5, rs.getInt("medium_layout_count"));
            mysqlStmt.setInt(6, rs.getInt("large_layout_count"));
            mysqlStmt.setInt(7, rs.getInt("premium_decoration_count"));
            mysqlStmt.setInt(8, rs.getInt("simple_decoration_count"));
            mysqlStmt.setInt(9, rs.getInt("rough_decoration_count"));

            // 处理 TIMESTAMP → DATETIME（Hive 返回 java.sql.Timestamp）
            Timestamp ts = rs.getTimestamp("analysis_time");
            mysqlStmt.setTimestamp(10, ts); // MySQL DATETIME 兼容 Timestamp

            mysqlStmt.setInt(11, rs.getInt("checkid"));
            mysqlStmt.setString(12, PT_DATE); // 显式设置 pt_date

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

    /** 获取年份区间分类的CASE WHEN表达式 */
    private static String getYearRangeCase(String buildYearField) {
        return "CASE \n" +
                " WHEN (cast(" + buildYearField + " as int) >= 1950 AND cast(" + buildYearField + " as int) <= 1970) THEN '1950-1970'\n" +
                " WHEN (cast(" + buildYearField + " as int) > 1970 AND cast(" + buildYearField + " as int) <= 1990) THEN '1970-1990'\n" +
                " WHEN (cast(" + buildYearField + " as int) > 1990 AND cast(" + buildYearField + " as int) <= 2000) THEN '1990-2000'\n" +
                " WHEN (cast(" + buildYearField + " as int) > 2000 AND cast(" + buildYearField + " as int) <= 2010) THEN '2000-2010'\n" +
                " WHEN (cast(" + buildYearField + " as int) > 2010 AND cast(" + buildYearField + " as int) <= 2020) THEN '2010-2020'\n" +
                " ELSE '其他年份' \n" +
                "END";
    }

    /** 设置Hive参数 */
    private static void setupHiveParameters(Connection conn) throws SQLException {
        System.out.println("\n设置Hive参数...");
        String[] hiveParams = {
                "SET hive.enforce.bucketing = true",
                "SET hive.exec.dynamic.partition = true",
                "SET hive.exec.dynamic.partition.mode = nonstrict",
                "SET hive.auto.convert.join = false",
                "SET hive.vectorized.execution.enabled = true",
                "SET hive.cbo.enable = true",
                "SET hive.exec.compress.output = true",
                "SET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec",
                "SET hive.exec.parallel = true",
                "SET hive.exec.parallel.thread.number = 4",
                "SET hive.map.aggr = true",
                "SET hive.groupby.skewindata = true"
        };
        for (String param : hiveParams) {
            try (PreparedStatement stmt = conn.prepareStatement(param)) {
                stmt.execute();
            }
        }
        System.out.println("✓ Hive参数设置完成");
    }

    /** 检查源表数据 */
    private static void checkSourceData(Connection conn) throws SQLException {
        System.out.println("\n检查源表数据...");
        String baseCheckSQL = String.format(
                "SELECT \n" +
                        " COUNT(*) as total_count,\n" +
                        " COUNT(DISTINCT district) as district_count,\n" +
                        " MIN(CAST(build_year AS INT)) as min_year,\n" +
                        " MAX(CAST(build_year AS INT)) as max_year,\n" +
                        " AVG(price_per_sqm) as avg_price,\n" +
                        " SUM(CASE WHEN elevator_int = 1 THEN 1 ELSE 0 END) as elevator_houses\n" +
                        "FROM %s.%s \n" +
                        "WHERE checkid = %d \n" +
                        " AND build_year REGEXP '^\\\\d{4}$'",
                DATABASE, SOURCE_TABLE, ANALYSIS_CHECK_ID
        );
        try (PreparedStatement stmt = conn.prepareStatement(baseCheckSQL); ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                System.out.println("├─ 总记录数: " + formatNumber(rs.getLong("total_count")));
                System.out.println("├─ 涉及区县数: " + rs.getInt("district_count"));
                System.out.println("├─ 最早建造年份: " + rs.getInt("min_year"));
                System.out.println("├─ 最晚建造年份: " + rs.getInt("max_year"));
                System.out.println("├─ 平均单价: " + String.format("%.2f", rs.getDouble("avg_price")) + "元/㎡");
                System.out.println("└─ 有电梯房屋数: " + formatNumber(rs.getLong("elevator_houses")));
            }
        }

        String yearDistributionSQL = String.format(
                "SELECT \n" +
                        " year_range,\n" +
                        " count\n" +
                        "FROM (\n" +
                        " SELECT \n" +
                        " %s as year_range,\n" +
                        " COUNT(*) as count\n" +
                        " FROM %s.%s \n" +
                        " WHERE checkid = %d \n" +
                        " AND build_year REGEXP '^\\\\d{4}$'\n" +
                        " GROUP BY %s\n" +
                        ") t\n" +
                        "ORDER BY \n" +
                        " CASE year_range\n" +
                        " WHEN '1950-1970' THEN 1\n" +
                        " WHEN '1970-1990' THEN 2\n" +
                        " WHEN '1990-2000' THEN 3\n" +
                        " WHEN '2000-2010' THEN 4\n" +
                        " WHEN '2010-2020' THEN 5\n" +
                        " ELSE 6\n" +
                        " END",
                getYearRangeCase("build_year"), DATABASE, SOURCE_TABLE, ANALYSIS_CHECK_ID, getYearRangeCase("build_year")
        );
        try (PreparedStatement stmt = conn.prepareStatement(yearDistributionSQL); ResultSet rs = stmt.executeQuery()) {
            System.out.println("\n年份区间分布预览:");
            printDashLine(40);
            int previewCount = 0;
            while (rs.next() && previewCount < 6) {
                System.out.println("├─ " + rs.getString("year_range") + ": " + formatNumber(rs.getLong("count")) + "套");
                previewCount++;
            }
            printDashLine(40);
        }
    }

    /** 执行复杂的数据分析并插入数据 */
    private static void executeComplexAnalysis(Connection conn) throws SQLException {
        System.out.println("\n执行复杂数据分析...");
        StringBuilder analysisSQL = new StringBuilder();
        analysisSQL.append("INSERT INTO TABLE ").append(DATABASE).append(".").append(TARGET_TABLE).append(" \n");
        analysisSQL.append("PARTITION (pt_date = '").append(PT_DATE).append("') \n");
        analysisSQL.append("\n");
        analysisSQL.append("SELECT \n");
        analysisSQL.append(" year_range,\n");
        analysisSQL.append(" house_count,\n");
        analysisSQL.append(" elevator_count,\n");
        analysisSQL.append(" small_layout_count,\n");
        analysisSQL.append(" medium_layout_count,\n");
        analysisSQL.append(" large_layout_count,\n");
        analysisSQL.append(" premium_decoration_count,\n");
        analysisSQL.append(" simple_decoration_count,\n");
        analysisSQL.append(" rough_decoration_count,\n");
        analysisSQL.append(" analysis_time,\n");
        analysisSQL.append(" checkid\n");
        analysisSQL.append("FROM (\n");
        analysisSQL.append(" SELECT \n");
        analysisSQL.append(" ").append(getYearRangeCase("COALESCE(build_year, '')")).append(" as year_range,\n");
        analysisSQL.append(" CAST(COUNT(*) AS INT) as house_count,\n");
        analysisSQL.append(" CAST(SUM(CASE WHEN elevator_int = 1 THEN 1 ELSE 0 END) AS INT) as elevator_count,\n");
        analysisSQL.append(" CAST(SUM(CASE \n");
        analysisSQL.append(" WHEN layout REGEXP '^1室' OR layout LIKE '1室%%' OR layout LIKE '1房%%' THEN 1\n");
        analysisSQL.append(" WHEN layout REGEXP '^2室' OR layout LIKE '2室%%' OR layout LIKE '2房%%' THEN 1\n");
        analysisSQL.append(" ELSE 0 \n");
        analysisSQL.append(" END) AS INT) as small_layout_count,\n");
        analysisSQL.append(" CAST(SUM(CASE \n");
        analysisSQL.append(" WHEN layout REGEXP '^3室' OR layout LIKE '3室%%' OR layout LIKE '3房%%' THEN 1\n");
        analysisSQL.append(" WHEN layout REGEXP '^4室' OR layout LIKE '4室%%' OR layout LIKE '4房%%' THEN 1\n");
        analysisSQL.append(" ELSE 0 \n");
        analysisSQL.append(" END) AS INT) as medium_layout_count,\n");
        analysisSQL.append(" CAST(SUM(CASE \n");
        analysisSQL.append(" WHEN layout REGEXP '^([5-9]|\\\\d{2,})室' THEN 1\n");
        analysisSQL.append(" WHEN layout LIKE '%%别墅%%' OR layout LIKE '%%复式%%' THEN 1\n");
        analysisSQL.append(" ELSE 0 \n");
        analysisSQL.append(" END) AS INT) as large_layout_count,\n");
        analysisSQL.append(" CAST(SUM(CASE WHEN decoration = '精装' THEN 1 ELSE 0 END) AS INT) as premium_decoration_count,\n");
        analysisSQL.append(" CAST(SUM(CASE WHEN decoration = '简装' THEN 1 ELSE 0 END) AS INT) as simple_decoration_count,\n");
        analysisSQL.append(" CAST(SUM(CASE WHEN decoration = '毛坯' THEN 1 ELSE 0 END) AS INT) as rough_decoration_count,\n");
        analysisSQL.append(" from_unixtime(unix_timestamp()) as analysis_time,\n");
        analysisSQL.append(" CAST(").append(ANALYSIS_CHECK_ID).append(" AS INT) as checkid\n");
        analysisSQL.append(" FROM (\n");
        analysisSQL.append(" SELECT \n");
        analysisSQL.append(" h.*\n");
        analysisSQL.append(" FROM ").append(DATABASE).append(".").append(SOURCE_TABLE).append(" h\n");
        analysisSQL.append(" WHERE checkid = ").append(ANALYSIS_CHECK_ID).append("\n");
        analysisSQL.append(" AND build_year IS NOT NULL\n");
        analysisSQL.append(" AND build_year != ''\n");
        analysisSQL.append(" AND layout IS NOT NULL\n");
        analysisSQL.append(" ) cleaned_data\n");
        analysisSQL.append(" GROUP BY ").append(getYearRangeCase("COALESCE(build_year, '')")).append("\n");
        analysisSQL.append(" HAVING COUNT(*) >= 1\n");
        analysisSQL.append(") final_result\n");
        analysisSQL.append("ORDER BY \n");
        analysisSQL.append(" CASE year_range\n");
        analysisSQL.append(" WHEN '1950-1970' THEN 1\n");
        analysisSQL.append(" WHEN '1970-1990' THEN 2\n");
        analysisSQL.append(" WHEN '1990-2000' THEN 3\n");
        analysisSQL.append(" WHEN '2000-2010' THEN 4\n");
        analysisSQL.append(" WHEN '2010-2020' THEN 5\n");
        analysisSQL.append(" ELSE 6\n" +
                " END");

        System.out.println("执行复杂分析SQL...");
        long startTime = System.currentTimeMillis();
        try (PreparedStatement stmt = conn.prepareStatement(analysisSQL.toString())) {
            int result = stmt.executeUpdate();
            long endTime = System.currentTimeMillis();
            System.out.println("✓ 复杂分析完成，耗时: " + (endTime - startTime) + "ms");
            System.out.println("✓ 数据已插入到分区: pt_date='" + PT_DATE + "'");
            System.out.println("✓ 检查批次: checkid=" + ANALYSIS_CHECK_ID);
            System.out.println("✓ 影响行数: " + result);
        } catch (SQLException e) {
            System.err.println("❌ 执行复杂SQL时出错: " + e.getMessage());
            System.err.println("\n尝试使用简化版本...");
            executeSimpleAnalysis(conn);
        }
    }

    /** 简化版本分析（备用方案） */
    private static void executeSimpleAnalysis(Connection conn) throws SQLException {
        System.out.println("使用简化版本SQL...");
        String yearRangeCase = getYearRangeCase("build_year");
        String simpleSQL = String.format(
                "INSERT OVERWRITE TABLE %s.%s \n" +
                        "PARTITION (pt_date = '%s') \n" +
                        "SELECT \n" +
                        " %s as year_range,\n" +
                        " CAST(COUNT(*) AS INT) as house_count,\n" +
                        " CAST(SUM(CASE WHEN elevator_int = 1 THEN 1 ELSE 0 END) AS INT) as elevator_count,\n" +
                        " CAST(SUM(CASE WHEN layout REGEXP '^[12]室' THEN 1 ELSE 0 END) AS INT) as small_layout_count,\n" +
                        " CAST(SUM(CASE WHEN layout REGEXP '^[34]室' THEN 1 ELSE 0 END) AS INT) as medium_layout_count,\n" +
                        " CAST(SUM(CASE WHEN layout REGEXP '^([5-9]|\\\\d{2,})室' OR layout LIKE '%%%%别墅%%%%' THEN 1 ELSE 0 END) AS INT) as large_layout_count,\n" +
                        " CAST(SUM(CASE WHEN decoration = '精装' THEN 1 ELSE 0 END) AS INT) as premium_decoration_count,\n" +
                        " CAST(SUM(CASE WHEN decoration = '简装' THEN 1 ELSE 0 END) AS INT) as simple_decoration_count,\n" +
                        " CAST(SUM(CASE WHEN decoration = '毛坯' THEN 1 ELSE 0 END) AS INT) as rough_decoration_count,\n" +
                        " from_unixtime(unix_timestamp()) as analysis_time,\n" +
                        " CAST(%d AS INT) as checkid\n" +
                        "FROM %s.%s \n" +
                        "WHERE checkid = %d \n" +
                        " AND build_year IS NOT NULL \n" +
                        " AND build_year != ''\n" +
                        "GROUP BY %s\n" +
                        "HAVING COUNT(*) >= 1\n" +
                        "ORDER BY \n" +
                        " CASE %s\n" +
                        " WHEN '1950-1970' THEN 1\n" +
                        " WHEN '1970-1990' THEN 2\n" +
                        " WHEN '1990-2000' THEN 3\n" +
                        " WHEN '2000-2010' THEN 4\n" +
                        " WHEN '2010-2020' THEN 5\n" +
                        " ELSE 6\n" +
                        " END",
                DATABASE, TARGET_TABLE, PT_DATE, yearRangeCase,
                ANALYSIS_CHECK_ID,
                DATABASE, SOURCE_TABLE, ANALYSIS_CHECK_ID,
                yearRangeCase,
                yearRangeCase
        );
        System.out.println("执行简化SQL...");
        long startTime = System.currentTimeMillis();
        try (PreparedStatement stmt = conn.prepareStatement(simpleSQL)) {
            int result = stmt.executeUpdate();
            long endTime = System.currentTimeMillis();
            System.out.println("✓ 简化分析完成，耗时: " + (endTime - startTime) + "ms");
            System.out.println("✓ 影响行数: " + result);
        } catch (SQLException e) {
            System.err.println("❌ 简化版本也失败: " + e.getMessage());
            throw e;
        }
    }

    /** 验证插入结果 */
    private static void verifyResults(Connection conn) throws SQLException {
        System.out.println("\n验证分析结果...");
        String verifySQL = String.format(
                "SELECT \n" +
                        " year_range,\n" +
                        " house_count,\n" +
                        " elevator_count,\n" +
                        " small_layout_count,\n" +
                        " medium_layout_count,\n" +
                        " large_layout_count,\n" +
                        " premium_decoration_count,\n" +
                        " simple_decoration_count,\n" +
                        " rough_decoration_count,\n" +
                        " checkid,\n" +
                        " pt_date,\n" +
                        " ROUND(elevator_count * 100.0 / house_count, 2) as elevator_ratio,\n" +
                        " ROUND(small_layout_count * 100.0 / house_count, 2) as small_ratio,\n" +
                        " ROUND(medium_layout_count * 100.0 / house_count, 2) as medium_ratio,\n" +
                        " ROUND(large_layout_count * 100.0 / house_count, 2) as large_ratio,\n" +
                        " ROUND((premium_decoration_count + simple_decoration_count) * 100.0 / house_count, 2) as decoration_ratio\n" +
                        "FROM %s.%s \n" +
                        "WHERE pt_date = '%s' \n" +
                        " AND checkid = %d \n" +
                        "ORDER BY \n" +
                        " CASE year_range\n" +
                        " WHEN '1950-1970' THEN 1\n" +
                        " WHEN '1970-1990' THEN 2\n" +
                        " WHEN '1990-2000' THEN 3\n" +
                        " WHEN '2000-2010' THEN 4\n" +
                        " WHEN '2010-2020' THEN 5\n" +
                        " ELSE 6\n" +
                        " END",
                DATABASE, TARGET_TABLE, PT_DATE, ANALYSIS_CHECK_ID
        );
        try (PreparedStatement stmt = conn.prepareStatement(verifySQL); ResultSet rs = stmt.executeQuery()) {
            System.out.println("\n📊 房屋年份分析结果 (checkid=" + ANALYSIS_CHECK_ID + "):");
            printLine(130);
            System.out.println(String.format("%-15s %-10s %-10s %-10s %-10s %-10s %-8s %-8s %-8s %-8s %-10s %-12s",
                    "年份区间", "房屋数", "电梯数", "小户型", "中户型", "大户型", "精装", "简装", "毛坯", "电梯比例%", "批次ID", "分区日期"));
            printDashLine(130);
            int totalHouses = 0;
            int totalElevators = 0;
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                System.out.println(String.format("%-15s %-10d %-10d %-10d %-10d %-10d %-8d %-8d %-8d %-8.1f %-10d %-12s",
                        rs.getString("year_range"),
                        rs.getInt("house_count"),
                        rs.getInt("elevator_count"),
                        rs.getInt("small_layout_count"),
                        rs.getInt("medium_layout_count"),
                        rs.getInt("large_layout_count"),
                        rs.getInt("premium_decoration_count"),
                        rs.getInt("simple_decoration_count"),
                        rs.getInt("rough_decoration_count"),
                        rs.getDouble("elevator_ratio"),
                        rs.getInt("checkid"),
                        rs.getString("pt_date")
                ));
                totalHouses += rs.getInt("house_count");
                totalElevators += rs.getInt("elevator_count");
            }
            printLine(130);
            System.out.println("\n📈 汇总统计 (checkid=" + ANALYSIS_CHECK_ID + "):");
            System.out.println("├─ 总分析年份区间数: " + rowCount);
            System.out.println("├─ 总房屋数量: " + formatNumber(totalHouses));
            System.out.println("├─ 总电梯数量: " + formatNumber(totalElevators));
            if (totalHouses > 0) {
                System.out.println("├─ 整体电梯比例: " + String.format("%.1f", totalElevators * 100.0 / totalHouses) + "%");
                displayYearDistributionSimple(conn, totalHouses);
            } else {
                System.out.println("├─ 整体电梯比例: 0.0%");
            }
        }
    }

    /** 显示年份区间分布 - 最简化版本 */
    private static void displayYearDistributionSimple(Connection conn, int totalHouses) throws SQLException {
        String distributionSQL = String.format(
                "SELECT \n" +
                        " year_range,\n" +
                        " house_count\n" +
                        "FROM %s.%s \n" +
                        "WHERE pt_date = '%s' \n" +
                        " AND checkid = %d \n" +
                        "ORDER BY \n" +
                        " CASE year_range\n" +
                        " WHEN '1950-1970' THEN 1\n" +
                        " WHEN '1970-1990' THEN 2\n" +
                        " WHEN '1990-2000' THEN 3\n" +
                        " WHEN '2000-2010' THEN 4\n" +
                        " WHEN '2010-2020' THEN 5\n" +
                        " ELSE 6\n" +
                        " END",
                DATABASE, TARGET_TABLE, PT_DATE, ANALYSIS_CHECK_ID
        );
        try (PreparedStatement stmt = conn.prepareStatement(distributionSQL); ResultSet rs = stmt.executeQuery()) {
            System.out.println("└─ 年份区间分布:");
            while (rs.next()) {
                int houseCount = rs.getInt("house_count");
                double percent = totalHouses > 0 ? houseCount * 100.0 / totalHouses : 0;
                System.out.println(" ├─ " + rs.getString("year_range") + ": " + formatNumber(houseCount) + "套 (" + String.format("%.1f", percent) + "%)");
            }
        }
    }

    private static void printLine(int length) {
        System.out.println(new String(new char[length]).replace('\0', '='));
    }

    private static void printDashLine(int length) {
        System.out.println(new String(new char[length]).replace('\0', '-'));
    }

    private static String formatNumber(long number) {
        return String.format("%,d", number);
    }
}