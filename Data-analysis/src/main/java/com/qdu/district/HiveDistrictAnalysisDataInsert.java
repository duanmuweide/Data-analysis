package com.qdu.district;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HiveDistrictAnalysisDataInsert {

    // Hive连接配置 - 在URL中指定用户
    private static final String HIVE_JDBC_URL = "jdbc:hive2://master-pc:10000/dataanalysis;user=master-pc";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // 表配置
    private static final String SOURCE_TABLE = "house_info_clean";
    private static final String TARGET_TABLE = "district_house_price_analysis";
    private static final String DATABASE = "dataanalysis";

    // 分区值
    private static final String PARTITION_VALUE;

    static {
        // 使用当前日期作为分区值
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        PARTITION_VALUE = sdf.format(new Date());
    }

    public static void main(String[] args) {
        // 设置Hadoop用户身份
        System.setProperty("HADOOP_USER_NAME", "master-pc");

        System.out.println("===== 当前Hadoop用户配置 =====");
        System.out.println("HADOOP_USER_NAME: " + System.getProperty("HADOOP_USER_NAME"));
        System.out.println("连接URL: " + HIVE_JDBC_URL);

        System.out.println("开始分析市区房价数据并插入到目标表...");
        System.out.println("分区日期: " + PARTITION_VALUE);

        Connection conn = null;

        try {
            // 1. 建立Hive连接
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            System.out.println("✓ Hive连接成功");

            // 2. 检查源表数据
            long sourceCount = checkSourceData(conn);
            if (sourceCount == 0) {
                System.out.println("源表没有数据，退出程序");
                return;
            }

            // 3. 执行复杂的数据分析并插入数据
            insertComplexAnalysisData(conn);

            // 4. 验证插入结果
            verifyInsertResult(conn);

            System.out.println("数据分析与插入完成！");

        } catch (Exception e) {
            System.err.println("数据处理过程中出现错误:");
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException e) { e.printStackTrace(); }
            }
        }
    }

    /**
     * 检查源表数据
     */
    private static long checkSourceData(Connection conn) throws SQLException {
        System.out.println("\n检查源表数据...");

        String countSQL = String.format(
                "SELECT COUNT(*) as total_count FROM %s.%s",
                DATABASE, SOURCE_TABLE
        );

        try (PreparedStatement stmt = conn.prepareStatement(countSQL);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                long count = rs.getLong("total_count");
                System.out.println("源表 " + SOURCE_TABLE + " 总记录数: " + count);
                return count;
            }
        }

        return 0;
    }

    /**
     * 执行复杂的数据分析并插入数据
     * 包含：分组、内置函数、聚合等操作
     */
    private static void insertComplexAnalysisData(Connection conn) throws SQLException {
        System.out.println("\n执行复杂数据分析并插入到目标表...");

        // 构建复杂的HQL语句（包含尽量多的功能）
        String complexSQL = buildComplexAnalysisSQL();

        System.out.println("执行复杂分析SQL:");
        printLine(80);
        System.out.println(complexSQL);
        printLine(80);

        long startTime = System.currentTimeMillis();

        try (PreparedStatement stmt = conn.prepareStatement(complexSQL)) {
            int result = stmt.executeUpdate();
            long endTime = System.currentTimeMillis();

            System.out.println("✓ 复杂数据分析插入成功，耗时: " + (endTime - startTime) + "ms");
            System.out.println("✓ 数据已插入到分区: load_date='" + PARTITION_VALUE + "'");
        } catch (SQLException e) {
            System.err.println("执行复杂SQL时出错: " + e.getMessage());
            System.err.println("尝试使用简化版本...");
            insertSimpleAnalysisData(conn);
        }
    }

    /**
     * 构建复杂的分析SQL语句
     * 包含：分组、内置函数、聚合等操作
     */
    private static String buildComplexAnalysisSQL() {
        // 使用StringBuilder避免String.format的格式问题
        StringBuilder sql = new StringBuilder();

        sql.append("INSERT OVERWRITE TABLE ").append(DATABASE).append(".").append(TARGET_TABLE);
        sql.append(" PARTITION (load_date='").append(PARTITION_VALUE).append("') \n");
        sql.append("SELECT \n");
        sql.append("    -- 分组字段：市区名称\n");
        sql.append("    district,\n");
        sql.append("    \n");
        sql.append("    -- 1. 平均房价（使用AVG函数 + 数学函数ROUND + 类型转换）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 聚合函数：计算平均值\n");
        sql.append("        AVG(\n");
        sql.append("            -- 条件函数：处理异常值\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm <= 0 THEN NULL\n");
        sql.append("                WHEN price_per_sqm > 500000 THEN 500000  -- 上限截断\n");
        sql.append("                ELSE CAST(price_per_sqm AS DOUBLE)\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as avg_price_per_sqm,\n");
        sql.append("    \n");
        sql.append("    -- 2. 房屋数量（使用COUNT聚合函数）\n");
        sql.append("    CAST(\n");
        sql.append("        -- 分组计数\n");
        sql.append("        COUNT(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN district IS NOT NULL AND price_per_sqm > 0 THEN 1\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        ) \n");
        sql.append("    AS INT) as house_count,\n");
        sql.append("    \n");
        sql.append("    -- 3. 最低单价（使用MIN聚合函数）\n");
        sql.append("    CAST(\n");
        sql.append("        -- 数学函数：求最小值\n");
        sql.append("        MIN(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    AS INT) as min_price,\n");
        sql.append("    \n");
        sql.append("    -- 4. 最高单价（使用MAX聚合函数）\n");
        sql.append("    CAST(\n");
        sql.append("        -- 数学函数：求最大值\n");
        sql.append("        MAX(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    AS INT) as max_price,\n");
        sql.append("    \n");
        sql.append("    -- 5. 中位数单价（使用PERCENTILE_APPROX函数 + 数学运算）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 统计函数：计算中位数（50%分位数）\n");
        sql.append("        CAST(\n");
        sql.append("            PERCENTILE_APPROX(\n");
        sql.append("                CAST(\n");
        sql.append("                    CASE \n");
        sql.append("                        WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                        ELSE NULL\n");
        sql.append("                    END \n");
        sql.append("                AS BIGINT), \n");
        sql.append("                0.5\n");
        sql.append("            ) \n");
        sql.append("        AS DOUBLE)\n");
        sql.append("    ) AS INT) as median_price,\n");
        sql.append("    \n");
        sql.append("    -- 6. 价格方差（使用VARIANCE函数 + 数学运算）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 统计函数：计算方差\n");
        sql.append("        VARIANCE(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as price_variance,\n");
        sql.append("    \n");
        sql.append("    -- 7. 价格标准差（使用STDDEV函数 + 数学运算）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 统计函数：计算标准差\n");
        sql.append("        STDDEV(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as std_price,\n");
        sql.append("    \n");
        sql.append("    -- 8. 平均房龄（使用AVG函数 + 数学运算）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 聚合函数：计算平均值\n");
        sql.append("        AVG(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN house_age > 0 AND house_age < 100 THEN house_age\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as avg_house_age,\n");
        sql.append("    \n");
        sql.append("    -- 9. 平均面积（使用AVG函数 + 数学运算）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        -- 聚合函数：计算平均值\n");
        sql.append("        AVG(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN area > 10 AND area < 1000 THEN area  -- 合理面积范围\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as avg_area\n");
        sql.append("    \n");
        sql.append("FROM ").append(DATABASE).append(".").append(SOURCE_TABLE).append("\n");
        sql.append("-- WHERE条件过滤：数据质量清洗\n");
        sql.append("WHERE district IS NOT NULL \n");
        sql.append("  AND district != '' \n");
        sql.append("  AND price_per_sqm IS NOT NULL\n");
        sql.append("  AND house_age IS NOT NULL\n");
        sql.append("  AND area IS NOT NULL\n");
        sql.append("-- GROUP BY分组：按市区分组\n");
        sql.append("GROUP BY district\n");
        sql.append("-- HAVING条件：分组后过滤\n");
        sql.append("HAVING \n");
        sql.append("    COUNT(*) >= 5  -- 至少5条记录\n");
        sql.append("    AND AVG(CASE WHEN price_per_sqm > 0 THEN price_per_sqm ELSE NULL END) > 10000  -- 平均价大于1万\n");
        sql.append("-- ORDER BY排序：按平均房价降序排列\n");
        sql.append("ORDER BY avg_price_per_sqm DESC");

        return sql.toString();
    }

    /**
     * 简化版本（如果复杂版本失败）
     */
    private static void insertSimpleAnalysisData(Connection conn) throws SQLException {
        System.out.println("使用简化版本SQL...");

        StringBuilder simpleSQL = new StringBuilder();
        simpleSQL.append("INSERT OVERWRITE TABLE ").append(DATABASE).append(".").append(TARGET_TABLE);
        simpleSQL.append(" PARTITION (load_date='").append(PARTITION_VALUE).append("') \n");
        simpleSQL.append("SELECT \n");
        simpleSQL.append("    district,\n");
        simpleSQL.append("    CAST(ROUND(AVG(price_per_sqm)) AS INT) as avg_price_per_sqm,\n");
        simpleSQL.append("    CAST(COUNT(*) AS INT) as house_count,\n");
        simpleSQL.append("    MIN(price_per_sqm) as min_price,\n");
        simpleSQL.append("    MAX(price_per_sqm) as max_price,\n");
        simpleSQL.append("    CAST(ROUND(AVG(price_per_sqm)) AS INT) as median_price,\n");
        simpleSQL.append("    0 as price_variance,\n");
        simpleSQL.append("    0 as std_price,\n");
        simpleSQL.append("    CAST(ROUND(AVG(house_age)) AS INT) as avg_house_age,\n");
        simpleSQL.append("    CAST(ROUND(AVG(area)) AS INT) as avg_area\n");
        simpleSQL.append("FROM ").append(DATABASE).append(".").append(SOURCE_TABLE).append("\n");
        simpleSQL.append("WHERE district IS NOT NULL \n");
        simpleSQL.append("  AND price_per_sqm > 0 \n");
        simpleSQL.append("GROUP BY district\n");
        simpleSQL.append("HAVING COUNT(*) > 0\n");
        simpleSQL.append("ORDER BY avg_price_per_sqm DESC");

        System.out.println("简化SQL:");
        printLine(80);
        System.out.println(simpleSQL.toString());
        printLine(80);

        long startTime = System.currentTimeMillis();

        try (PreparedStatement stmt = conn.prepareStatement(simpleSQL.toString())) {
            int result = stmt.executeUpdate();
            long endTime = System.currentTimeMillis();

            System.out.println("✓ 简化版本数据插入成功，耗时: " + (endTime - startTime) + "ms");
        }
    }

    /**
     * 验证插入结果
     */
    private static void verifyInsertResult(Connection conn) throws SQLException {
        System.out.println("\n验证插入结果...");

        // 1. 检查目标表记录数
        StringBuilder countSQL = new StringBuilder();
        countSQL.append("SELECT \n");
        countSQL.append("    COUNT(*) as inserted_count,\n");
        countSQL.append("    SUM(house_count) as total_houses,\n");
        countSQL.append("    AVG(avg_price_per_sqm) as avg_price,\n");
        countSQL.append("    MIN(avg_price_per_sqm) as min_avg_price,\n");
        countSQL.append("    MAX(avg_price_per_sqm) as max_avg_price\n");
        countSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE);
        countSQL.append(" WHERE load_date='").append(PARTITION_VALUE).append("'");

        try (PreparedStatement stmt = conn.prepareStatement(countSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                long insertedCount = rs.getLong("inserted_count");
                System.out.println("目标表插入记录数: " + insertedCount);
                System.out.println("总房屋数量: " + formatNumber(rs.getLong("total_houses")));
                System.out.println("平均房价: " + formatNumber(rs.getDouble("avg_price")) + "元/㎡");
                System.out.println("最低平均价: " + formatNumber(rs.getInt("min_avg_price")) + "元/㎡");
                System.out.println("最高平均价: " + formatNumber(rs.getInt("max_avg_price")) + "元/㎡");

                if (insertedCount == 0) {
                    System.out.println("警告：没有插入任何数据！");
                    return;
                }
            }
        }

        // 2. 查看详细的统计结果
        System.out.println("\n市区房价分析结果 (前10条):");
        printLine(140);

        StringBuilder sampleSQL = new StringBuilder();
        sampleSQL.append("SELECT \n");
        sampleSQL.append("    district,\n");
        sampleSQL.append("    avg_price_per_sqm,\n");
        sampleSQL.append("    house_count,\n");
        sampleSQL.append("    min_price,\n");
        sampleSQL.append("    max_price,\n");
        sampleSQL.append("    median_price,\n");
        sampleSQL.append("    price_variance,\n");
        sampleSQL.append("    std_price,\n");
        sampleSQL.append("    avg_house_age,\n");
        sampleSQL.append("    avg_area,\n");
        sampleSQL.append("    (max_price - min_price) as price_range\n");
        sampleSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE).append(" \n");
        sampleSQL.append("WHERE load_date='").append(PARTITION_VALUE).append("'\n");
        sampleSQL.append("ORDER BY avg_price_per_sqm DESC\n");
        sampleSQL.append("LIMIT 10");

        try (PreparedStatement stmt = conn.prepareStatement(sampleSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            System.out.println(String.format("%-12s %-12s %-10s %-10s %-10s %-12s %-12s %-12s %-10s %-10s %-10s",
                    "市区", "平均房价", "房屋数", "最低价", "最高价", "中位数", "方差", "标准差", "房龄", "面积", "价格范围"));
            printDashLine(140);

            while (rs.next()) {
                System.out.println(String.format("%-12s %-12d %-10d %-10d %-10d %-12d %-12d %-12d %-10d %-10d %-10d",
                        rs.getString("district"),
                        rs.getInt("avg_price_per_sqm"),
                        rs.getInt("house_count"),
                        rs.getInt("min_price"),
                        rs.getInt("max_price"),
                        rs.getInt("median_price"),
                        rs.getInt("price_variance"),
                        rs.getInt("std_price"),
                        rs.getInt("avg_house_age"),
                        rs.getInt("avg_area"),
                        rs.getInt("price_range")
                ));
            }
            printLine(140);
        }

        // 3. 高级统计分析
        System.out.println("\n高级统计分析:");

        StringBuilder statsSQL = new StringBuilder();
        statsSQL.append("SELECT \n");
        statsSQL.append("    COUNT(*) as district_count,\n");
        statsSQL.append("    SUM(house_count) as total_houses,\n");
        statsSQL.append("    AVG(avg_price_per_sqm) as city_avg_price,\n");
        statsSQL.append("    STDDEV(avg_price_per_sqm) as price_stddev,\n");
        statsSQL.append("    MIN(avg_price_per_sqm) as min_district_avg,\n");
        statsSQL.append("    MAX(avg_price_per_sqm) as max_district_avg,\n");
        statsSQL.append("    SUM(CASE WHEN avg_price_per_sqm < 50000 THEN 1 ELSE 0 END) as low_price,\n");
        statsSQL.append("    SUM(CASE WHEN avg_price_per_sqm BETWEEN 50000 AND 80000 THEN 1 ELSE 0 END) as medium_price,\n");
        statsSQL.append("    SUM(CASE WHEN avg_price_per_sqm > 80000 THEN 1 ELSE 0 END) as high_price,\n");
        statsSQL.append("    AVG(avg_house_age) as avg_house_age_all,\n");
        statsSQL.append("    AVG(avg_area) as avg_area_all\n");
        statsSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE);
        statsSQL.append(" WHERE load_date='").append(PARTITION_VALUE).append("'");

        try (PreparedStatement stmt = conn.prepareStatement(statsSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                int districtCount = rs.getInt("district_count");
                System.out.println("统计的市区数量: " + districtCount);
                System.out.println("总房屋数量: " + formatNumber(rs.getLong("total_houses")));
                System.out.println("全市平均房价: " + formatNumber(rs.getDouble("city_avg_price")) + "元/㎡");
                System.out.println("房价标准差: " + formatNumber(rs.getDouble("price_stddev")) + "元/㎡");
                System.out.println("最低市区平均价: " + formatNumber(rs.getInt("min_district_avg")) + "元/㎡");
                System.out.println("最高市区平均价: " + formatNumber(rs.getInt("max_district_avg")) + "元/㎡");
                System.out.println("\n价格区间分布:");
                System.out.println("  低房价区(<5万): " + rs.getInt("low_price") + "个市区");
                System.out.println("  中房价区(5-8万): " + rs.getInt("medium_price") + "个市区");
                System.out.println("  高房价区(>8万): " + rs.getInt("high_price") + "个市区");
                System.out.println("\n其他统计:");
                System.out.println("  平均房龄: " + String.format("%.1f", rs.getDouble("avg_house_age_all")) + "年");
                System.out.println("  平均面积: " + String.format("%.1f", rs.getDouble("avg_area_all")) + "㎡");

                // 计算百分比
                if (districtCount > 0) {
                    System.out.println("\n价格区间百分比:");
                    System.out.println("  低房价区: " + String.format("%.1f%%", rs.getInt("low_price") * 100.0 / districtCount));
                    System.out.println("  中房价区: " + String.format("%.1f%%", rs.getInt("medium_price") * 100.0 / districtCount));
                    System.out.println("  高房价区: " + String.format("%.1f%%", rs.getInt("high_price") * 100.0 / districtCount));
                }
            }
        }
    }

    /**
     * 打印分隔线
     */
    private static void printLine(int length) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < length; i++) {
            line.append("=");
        }
        System.out.println(line.toString());
    }

    /**
     * 打印虚线分隔线
     */
    private static void printDashLine(int length) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < length; i++) {
            line.append("-");
        }
        System.out.println(line.toString());
    }

    /**
     * 格式化数字显示（添加千位分隔符）
     */
    private static String formatNumber(double number) {
        // 简单实现：添加千位分隔符
        String numStr = String.format("%.0f", number);
        StringBuilder result = new StringBuilder();

        int len = numStr.length();
        for (int i = 0; i < len; i++) {
            if (i > 0 && (len - i) % 3 == 0) {
                result.append(",");
            }
            result.append(numStr.charAt(i));
        }

        return result.toString();
    }

    /**
     * 格式化数字显示（整数版本）
     */
    private static String formatNumber(int number) {
        return formatNumber((double) number);
    }

    /**
     * 格式化数字显示（长整数版本）
     */
    private static String formatNumber(long number) {
        return formatNumber((double) number);
    }
}