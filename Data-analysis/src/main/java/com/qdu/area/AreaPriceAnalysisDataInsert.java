package com.qdu.area;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AreaPriceAnalysisDataInsert {

    // Hive连接配置
    private static final String HIVE_JDBC_URL = "jdbc:hive2://master-pc:10000/dataanalysis;user=master-pc";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // 表配置
    private static final String SOURCE_TABLE = "house_info_clean";
    private static final String TARGET_TABLE = "area_price_analysis";
    private static final String DATABASE = "dataanalysis";

    // 分区值
    private static final String PARTITION_VALUE;

    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        PARTITION_VALUE = sdf.format(new Date());
    }

    public static void main(String[] args) {
        // 设置Hadoop用户身份
        System.setProperty("HADOOP_USER_NAME", "master-pc");

        System.out.println("===== 面积区间房价分析程序（分桶表版本）=====");
        System.out.println("分区日期: " + PARTITION_VALUE);

        Connection conn = null;

        try {
            // 1. 建立Hive连接
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            System.out.println("✓ Hive连接成功");

            // 2. 设置分桶和动态分区参数
            setupHiveParameters(conn);

            // 3. 检查源表数据
            long sourceCount = checkSourceData(conn);
            if (sourceCount == 0) {
                System.out.println("源表没有数据，退出程序");
                return;
            }

            // 4. 执行复杂的数据分析并插入数据
            insertComplexAreaAnalysisData(conn);

            // 5. 验证插入结果
            verifyInsertResult(conn);

            System.out.println("面积区间房价分析完成！");

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
     * 设置Hive参数（针对分桶表和事务表）
     */
    private static void setupHiveParameters(Connection conn) throws SQLException {
        System.out.println("设置Hive参数...");

        // 设置分桶相关参数
        String[] hiveParams = {
                "SET hive.enforce.bucketing = true",
                "SET hive.exec.dynamic.partition = true",
                "SET hive.exec.dynamic.partition.mode = nonstrict",
                "SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager",
                "SET hive.compactor.initiator.on = true",
                "SET hive.compactor.worker.threads = 1",
                "SET hive.support.concurrency = true"
        };

        for (String param : hiveParams) {
            try (PreparedStatement stmt = conn.prepareStatement(param)) {
                stmt.execute();
            }
        }
        System.out.println("✓ Hive参数设置完成");
    }

    /**
     * 检查源表数据
     */
    private static long checkSourceData(Connection conn) throws SQLException {
        System.out.println("\n检查源表数据...");

        String countSQL = String.format(
                "SELECT COUNT(*) as total_count FROM %s.%s WHERE area > 0 AND price_per_sqm > 0",
                DATABASE, SOURCE_TABLE
        );

        try (PreparedStatement stmt = conn.prepareStatement(countSQL);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                long count = rs.getLong("total_count");
                System.out.println("有效房屋数据记录数: " + count);
                return count;
            }
        }

        return 0;
    }

    /**
     * 执行复杂的面积分析并插入数据
     */
    private static void insertComplexAreaAnalysisData(Connection conn) throws SQLException {
        System.out.println("\n执行复杂的面积区间房价分析...");

        // 构建复杂的HQL语句
        String complexSQL = buildComplexAreaAnalysisSQL();

        System.out.println("执行复杂分析SQL:");
        printLine(100);
        System.out.println(complexSQL);
        printLine(100);

        long startTime = System.currentTimeMillis();

        try (PreparedStatement stmt = conn.prepareStatement(complexSQL)) {
            int result = stmt.executeUpdate();
            long endTime = System.currentTimeMillis();

            System.out.println("✓ 复杂面积分析插入成功，耗时: " + (endTime - startTime) + "ms");
            System.out.println("✓ 数据已插入到分区: pt_date='" + PARTITION_VALUE + "'");
        } catch (SQLException e) {
            System.err.println("执行复杂SQL时出错: " + e.getMessage());
            System.err.println("尝试使用简化版本...");
            insertSimpleAreaAnalysisData(conn);
        }
    }

    /**
     * 构建复杂的面积分析SQL语句
     * 包含：子查询、窗口函数、聚合函数、内置函数等
     */
    private static String buildComplexAreaAnalysisSQL() {
        StringBuilder sql = new StringBuilder();

        sql.append("-- 使用INSERT OVERWRITE覆盖写入目标表分区（支持分桶表）\n");
        sql.append("INSERT OVERWRITE TABLE ").append(DATABASE).append(".").append(TARGET_TABLE);
        sql.append(" PARTITION (pt_date) \n\n");

        sql.append("SELECT \n");
        sql.append("    -- 分桶字段必须放在最前面\n");
        sql.append("    area_range,\n");
        sql.append("    \n");
        sql.append("    -- 1. 房屋数量\n");
        sql.append("    CAST(COUNT(*) AS INT) as house_count,\n");
        sql.append("    \n");
        sql.append("    -- 2. 平均单价\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        AVG(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm <= 0 THEN NULL\n");
        sql.append("                WHEN price_per_sqm > 500000 THEN 500000\n");
        sql.append("                ELSE CAST(price_per_sqm AS DOUBLE)\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as avg_price_per_sqm,\n");
        sql.append("    \n");
        sql.append("    -- 3. 最低单价\n");
        sql.append("    CAST(\n");
        sql.append("        MIN(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    AS INT) as min_price,\n");
        sql.append("    \n");
        sql.append("    -- 4. 最高单价\n");
        sql.append("    CAST(\n");
        sql.append("        MAX(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    AS INT) as max_price,\n");
        sql.append("    \n");
        sql.append("    -- 5. 中位数单价（使用PERCENTILE_APPROX函数）\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        PERCENTILE_APPROX(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN price_per_sqm > 0 THEN price_per_sqm\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END \n");
        sql.append("            AS BIGINT), \n");
        sql.append("            0.5\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as median_price,\n");
        sql.append("    \n");
        sql.append("    -- 6. 价格方差\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        VARIANCE(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN price_per_sqm > 0 AND price_per_sqm <= 500000 \n");
        sql.append("                    THEN price_per_sqm\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as price_variance,\n");
        sql.append("    \n");
        sql.append("    -- 7. 价格标准差\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        STDDEV(\n");
        sql.append("            CAST(\n");
        sql.append("                CASE \n");
        sql.append("                    WHEN price_per_sqm > 0 AND price_per_sqm <= 500000 \n");
        sql.append("                    THEN price_per_sqm\n");
        sql.append("                    ELSE NULL\n");
        sql.append("                END\n");
        sql.append("            AS DOUBLE)\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as price_stddev,\n");
        sql.append("    \n");
        sql.append("    -- 8. 平均房龄\n");
        sql.append("    CAST(ROUND(\n");
        sql.append("        AVG(\n");
        sql.append("            CASE \n");
        sql.append("                WHEN house_age >= 0 AND house_age <= 100 \n");
        sql.append("                THEN CAST(house_age AS DOUBLE)\n");
        sql.append("                ELSE NULL\n");
        sql.append("            END\n");
        sql.append("        )\n");
        sql.append("    ) AS INT) as avg_house_age,\n");
        sql.append("    \n");
        sql.append("    -- 9. 加载日期\n");
        sql.append("    '").append(PARTITION_VALUE).append("' as load_date,\n");
        sql.append("    \n");
        sql.append("    -- 10. 分区字段（必须放在最后）\n");
        sql.append("    '").append(PARTITION_VALUE).append("' as pt_date\n");
        sql.append("    \n");
        sql.append("FROM (\n");
        sql.append("    -- 子查询：对房屋按面积区间分类\n");
        sql.append("    SELECT \n");
        sql.append("        CASE \n");
        sql.append("            WHEN area < 50 THEN '50㎡以下'\n");
        sql.append("            WHEN area >= 50 AND area < 90 THEN '50-90㎡'\n");
        sql.append("            WHEN area >= 90 AND area < 144 THEN '90-144㎡'\n");
        sql.append("            WHEN area >= 144 AND area < 236 THEN '144-236㎡'\n");
        sql.append("            ELSE '236㎡以上'\n");
        sql.append("        END as area_range,\n");
        sql.append("        price_per_sqm,\n");
        sql.append("        house_age\n");
        sql.append("    FROM ").append(DATABASE).append(".").append(SOURCE_TABLE).append("\n");
        sql.append("    WHERE area IS NOT NULL AND area > 0\n");
        sql.append("      AND price_per_sqm IS NOT NULL AND price_per_sqm > 0\n");
        sql.append("      AND house_age IS NOT NULL AND house_age >= 0\n");
        sql.append(") classified_data\n");
        sql.append("GROUP BY area_range\n");
        sql.append("HAVING COUNT(*) >= 10  -- 至少有10条记录\n");
        sql.append("ORDER BY \n");
        sql.append("    CASE area_range\n");
        sql.append("        WHEN '50㎡以下' THEN 1\n");
        sql.append("        WHEN '50-90㎡' THEN 2\n");
        sql.append("        WHEN '90-144㎡' THEN 3\n");
        sql.append("        WHEN '144-236㎡' THEN 4\n");
        sql.append("        ELSE 5\n");
        sql.append("    END");

        return sql.toString();
    }

    /**
     * 简化版本（如果复杂版本失败）
     */
    private static void insertSimpleAreaAnalysisData(Connection conn) throws SQLException {
        System.out.println("使用简化版本SQL...");

        StringBuilder simpleSQL = new StringBuilder();
        simpleSQL.append("INSERT OVERWRITE TABLE ").append(DATABASE).append(".").append(TARGET_TABLE);
        simpleSQL.append(" PARTITION (pt_date) \n");
        simpleSQL.append("SELECT \n");
        simpleSQL.append("    CASE \n");
        simpleSQL.append("        WHEN area < 50 THEN '50㎡以下'\n");
        simpleSQL.append("        WHEN area >= 50 AND area < 90 THEN '50-90㎡'\n");
        simpleSQL.append("        WHEN area >= 90 AND area < 144 THEN '90-144㎡'\n");
        simpleSQL.append("        WHEN area >= 144 AND area < 236 THEN '144-236㎡'\n");
        simpleSQL.append("        ELSE '236㎡以上'\n");
        simpleSQL.append("    END as area_range,\n");
        simpleSQL.append("    CAST(COUNT(*) AS INT) as house_count,\n");
        simpleSQL.append("    CAST(ROUND(AVG(price_per_sqm)) AS INT) as avg_price_per_sqm,\n");
        simpleSQL.append("    CAST(MIN(price_per_sqm) AS INT) as min_price,\n");
        simpleSQL.append("    CAST(MAX(price_per_sqm) AS INT) as max_price,\n");
        simpleSQL.append("    CAST(ROUND(AVG(price_per_sqm)) AS INT) as median_price,\n");
        simpleSQL.append("    0 as price_variance,\n");
        simpleSQL.append("    0 as price_stddev,\n");
        simpleSQL.append("    CAST(ROUND(AVG(house_age)) AS INT) as avg_house_age,\n");
        simpleSQL.append("    '").append(PARTITION_VALUE).append("' as load_date,\n");
        simpleSQL.append("    '").append(PARTITION_VALUE).append("' as pt_date\n");
        simpleSQL.append("FROM ").append(DATABASE).append(".").append(SOURCE_TABLE).append("\n");
        simpleSQL.append("WHERE area > 0 AND price_per_sqm > 0\n");
        simpleSQL.append("GROUP BY \n");
        simpleSQL.append("    CASE \n");
        simpleSQL.append("        WHEN area < 50 THEN '50㎡以下'\n");
        simpleSQL.append("        WHEN area >= 50 AND area < 90 THEN '50-90㎡'\n");
        simpleSQL.append("        WHEN area >= 90 AND area < 144 THEN '90-144㎡'\n");
        simpleSQL.append("        WHEN area >= 144 AND area < 236 THEN '144-236㎡'\n");
        simpleSQL.append("        ELSE '236㎡以上'\n");
        simpleSQL.append("    END\n");
        simpleSQL.append("HAVING COUNT(*) >= 10\n");

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
        System.out.println("\n验证面积区间房价分析结果...");

        // 1. 检查目标表记录数
        StringBuilder countSQL = new StringBuilder();
        countSQL.append("SELECT \n");
        countSQL.append("    COUNT(*) as area_range_count,\n");
        countSQL.append("    SUM(house_count) as total_houses,\n");
        countSQL.append("    AVG(avg_price_per_sqm) as overall_avg_price,\n");
        countSQL.append("    MIN(avg_price_per_sqm) as min_avg_price,\n");
        countSQL.append("    MAX(avg_price_per_sqm) as max_avg_price,\n");
        countSQL.append("    STDDEV(avg_price_per_sqm) as price_stddev_avg\n");
        countSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE);
        countSQL.append(" WHERE pt_date='").append(PARTITION_VALUE).append("'");

        try (PreparedStatement stmt = conn.prepareStatement(countSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                int rangeCount = rs.getInt("area_range_count");
                System.out.println("面积区间数量: " + rangeCount);
                System.out.println("总房屋数量: " + formatNumber(rs.getLong("total_houses")));
                System.out.println("整体平均单价: " + formatNumber(rs.getDouble("overall_avg_price")) + "元/㎡");
                System.out.println("最低区间平均价: " + formatNumber(rs.getInt("min_avg_price")) + "元/㎡");
                System.out.println("最高区间平均价: " + formatNumber(rs.getInt("max_avg_price")) + "元/㎡");
                System.out.println("区间平均价标准差: " + formatNumber(rs.getDouble("price_stddev_avg")) + "元/㎡");

                if (rangeCount == 0) {
                    System.out.println("警告：没有插入任何数据！");
                    return;
                }
            }
        }

        // 2. 查看详细的面积区间分析结果
        System.out.println("\n面积区间房价分析详细结果:");
        printLine(120);

        StringBuilder sampleSQL = new StringBuilder();
        sampleSQL.append("SELECT \n");
        sampleSQL.append("    area_range,\n");
        sampleSQL.append("    house_count,\n");
        sampleSQL.append("    avg_price_per_sqm,\n");
        sampleSQL.append("    min_price,\n");
        sampleSQL.append("    max_price,\n");
        sampleSQL.append("    median_price,\n");
        sampleSQL.append("    price_stddev,\n");
        sampleSQL.append("    avg_house_age,\n");
        sampleSQL.append("    ROUND(house_count * 100.0 / SUM(house_count) OVER (), 1) as percentage,\n");
        sampleSQL.append("    (max_price - min_price) as price_range\n");
        sampleSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE).append(" \n");
        sampleSQL.append("WHERE pt_date='").append(PARTITION_VALUE).append("'\n");
        sampleSQL.append("ORDER BY \n");
        sampleSQL.append("    CASE area_range\n");
        sampleSQL.append("        WHEN '50㎡以下' THEN 1\n");
        sampleSQL.append("        WHEN '50-90㎡' THEN 2\n");
        sampleSQL.append("        WHEN '90-144㎡' THEN 3\n");
        sampleSQL.append("        WHEN '144-236㎡' THEN 4\n");
        sampleSQL.append("        ELSE 5\n");
        sampleSQL.append("    END");

        try (PreparedStatement stmt = conn.prepareStatement(sampleSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            System.out.println(String.format("%-15s %-10s %-12s %-10s %-10s %-12s %-10s %-8s %-8s %-10s",
                    "面积区间", "房屋数量", "平均单价", "最低价", "最高价", "中位数", "标准差", "房龄", "占比%", "价格范围"));
            printDashLine(120);

            while (rs.next()) {
                System.out.println(String.format("%-15s %-10d %-12d %-10d %-10d %-12d %-10d %-8d %-8.1f%% %-10d",
                        rs.getString("area_range"),
                        rs.getInt("house_count"),
                        rs.getInt("avg_price_per_sqm"),
                        rs.getInt("min_price"),
                        rs.getInt("max_price"),
                        rs.getInt("median_price"),
                        rs.getInt("price_stddev"),
                        rs.getInt("avg_house_age"),
                        rs.getDouble("percentage"),
                        rs.getInt("price_range")
                ));
            }
            printLine(120);
        }

        // 3. 各面积区间的价格分布情况
        System.out.println("\n各面积区间价格稳定性分析:");

        StringBuilder distributionSQL = new StringBuilder();
        distributionSQL.append("SELECT \n");
        distributionSQL.append("    area_range,\n");
        distributionSQL.append("    house_count,\n");
        distributionSQL.append("    avg_price_per_sqm,\n");
        distributionSQL.append("    price_stddev,\n");
        distributionSQL.append("    ROUND(price_stddev * 100.0 / avg_price_per_sqm, 1) as stddev_ratio,\n");
        distributionSQL.append("    CASE \n");
        distributionSQL.append("        WHEN price_stddev < avg_price_per_sqm * 0.1 THEN '非常稳定'\n");
        distributionSQL.append("        WHEN price_stddev < avg_price_per_sqm * 0.2 THEN '稳定'\n");
        distributionSQL.append("        WHEN price_stddev < avg_price_per_sqm * 0.3 THEN '一般'\n");
        distributionSQL.append("        ELSE '波动大'\n");
        distributionSQL.append("    END as price_stability\n");
        distributionSQL.append("FROM ").append(DATABASE).append(".").append(TARGET_TABLE).append("\n");
        distributionSQL.append("WHERE pt_date='").append(PARTITION_VALUE).append("'\n");
        distributionSQL.append("ORDER BY \n");
        distributionSQL.append("    CASE area_range\n");
        distributionSQL.append("        WHEN '50㎡以下' THEN 1\n");
        distributionSQL.append("        WHEN '50-90㎡' THEN 2\n");
        distributionSQL.append("        WHEN '90-144㎡' THEN 3\n");
        distributionSQL.append("        WHEN '144-236㎡' THEN 4\n");
        distributionSQL.append("        ELSE 5\n");
        distributionSQL.append("    END");

        try (PreparedStatement stmt = conn.prepareStatement(distributionSQL.toString());
             ResultSet rs = stmt.executeQuery()) {

            System.out.println(String.format("%-15s %-10s %-12s %-10s %-15s %-12s",
                    "面积区间", "房屋数量", "平均单价", "标准差", "标准差/平均(%)", "价格稳定性"));
            printDashLine(80);

            while (rs.next()) {
                System.out.println(String.format("%-15s %-10d %-12d %-10d %-15.1f%% %-12s",
                        rs.getString("area_range"),
                        rs.getInt("house_count"),
                        rs.getInt("avg_price_per_sqm"),
                        rs.getInt("price_stddev"),
                        rs.getDouble("stddev_ratio"),
                        rs.getString("price_stability")
                ));
            }
            printLine(80);
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
