package com.qdu.area;

import groovy.transform.builder.InitializerStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static jdk.nashorn.internal.runtime.PropertyDescriptor.SET;

public class AreaPriceAnalysisDataInsert {

    // === Hive 配置 ===
    private static final String HIVE_JDBC_URL = "jdbc:hive2://hadoop101:10000/cjz;user=master";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    // === MySQL 配置 ===
    private static final String MYSQL_JDBC_URL =
            "jdbc:mysql://192.168.211.1:3306/cjz?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "root";       //
    private static final String MYSQL_PASSWORD = "root"; //

    private static final String SOURCE_TABLE = "house_info_clean_checkid";
    private static final String TARGET_TABLE = "area_price_analysis";
    private static final String DATABASE = "cjz";
    private static final int ANALYSIS_CHECKID = 1;

    private static final String PARTITION_VALUE;
    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        PARTITION_VALUE = sdf.format(new Date());
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "master");
        System.out.println("===== 面积区间房价分析（无UDF + 含checkid）=====");
        System.out.println("分析批次: checkid = " + ANALYSIS_CHECKID);
        System.out.println("分区日期: " + PARTITION_VALUE);

        Connection hiveConn = null;
        Connection mysqlConn = null;

        try {
            // === 1. 连接 Hive ===
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            hiveConn = DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
            System.out.println("✓ Hive连接成功");

            setupHiveParameters(hiveConn);

            System.out.println("🔍 正在检查源表数据量...");
            long sourceCount = checkSourceData(hiveConn);
            if (sourceCount == 0) {
                System.out.println("⚠️ 源表无有效数据（checkid=" + ANALYSIS_CHECKID + "），退出");
                return;
            }
            System.out.println("✅ 源表有效数据量: " + sourceCount + " 条");

            // === 2. 执行 Hive 分析并写入 ===
            insertAreaAnalysisData(hiveConn);
            executeValidation(hiveConn);
            System.out.println("\n✅ Hive分析完成！数据已写入分区 pt_date='" + PARTITION_VALUE + "', checkid=" + ANALYSIS_CHECKID);

            // === 3. 连接 MySQL ===
            Class.forName("com.mysql.cj.jdbc.Driver");
            mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("✓ MySQL连接成功");

            // === 4. 从 Hive 读取刚写入的数据，并写入 MySQL ===
            syncHiveToMysql(hiveConn, mysqlConn);

            System.out.println("✅ 数据已同步至 MySQL 表 " + TARGET_TABLE);

        } catch (Exception e) {
            System.err.println("❌ 执行失败:");
            e.printStackTrace();
        } finally {
            // 关闭连接
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

    private static void setupHiveParameters(Connection conn) throws SQLException {
        String[] params = {
                "SET hive.exec.dynamic.partition = true",
                "SET hive.exec.dynamic.partition.mode = nonstrict",
                "SET hive.vectorized.execution.enabled = true",
                "SET hive.cbo.enable = true",
                "SET hive.fetch.task.conversion = more",

                // === 新增内存配置 ===
                "SET mapreduce.map.memory.mb=4096",
                "SET mapreduce.reduce.memory.mb=4096",
                "SET mapreduce.map.java.opts=-Xmx3276m",
                "SET mapreduce.reduce.java.opts=-Xmx3276m"

        };
        for (String param : params) {
            try (PreparedStatement stmt = conn.prepareStatement(param)) {
                stmt.execute();
                System.out.println("✓ 已设置: " + param);
            }
        }
    }

    private static long checkSourceData(Connection conn) throws SQLException {
        String sql = String.format(
                "SELECT COUNT(*) FROM %s.%s WHERE area > 0 AND price_per_sqm > 0 AND checkid = %d",
                DATABASE, SOURCE_TABLE, ANALYSIS_CHECKID
        );
        System.out.println("Executing: " + sql);
        long start = System.currentTimeMillis();
        try (PreparedStatement stmt = conn.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                long count = rs.getLong(1);
                System.out.println("⏱️ checkSourceData 耗时: " + (System.currentTimeMillis() - start) + "ms");
                return count;
            }
        }
        return 0;
    }

    private static void insertAreaAnalysisData(Connection conn) throws SQLException {
        String sql = "WITH base_data AS (\n" +
                " SELECT \n" +
                " CASE\n" +
                " WHEN area < 50 THEN '50㎡以下'\n" +
                " WHEN area < 90 THEN '50-90㎡'\n" +
                " WHEN area < 144 THEN '90-144㎡'\n" +
                " WHEN area < 236 THEN '144-236㎡'\n" +
                " ELSE '236㎡以上'\n" +
                " END AS area_range,\n" +
                " price_per_sqm,\n" +
                " house_age\n" +
                " FROM " + DATABASE + "." + SOURCE_TABLE + "\n" +
                " WHERE area > 0 AND price_per_sqm > 0 AND checkid = " + ANALYSIS_CHECKID + "\n" +
                "),\n" +
                "agg_summary AS (\n" +
                " SELECT \n" +
                " area_range,\n" +
                " COUNT(*) AS house_count,\n" +
                " CAST(AVG(price_per_sqm) AS INT) AS avg_price_per_sqm,\n" +
                " MIN(price_per_sqm) AS min_price,\n" +
                " MAX(price_per_sqm) AS max_price,\n" +
                " CAST(AVG(house_age) AS INT) AS avg_house_age\n" +
                " FROM base_data\n" +
                " GROUP BY area_range\n" +
                " HAVING COUNT(*) >= 10\n" +
                ")\n" +
                "INSERT INTO TABLE " + DATABASE + "." + TARGET_TABLE + " PARTITION (pt_date = '" + PARTITION_VALUE + "')\n" +
                "SELECT \n" +
                " a.area_range,\n" +
                " a.house_count,\n" +
                " a.avg_price_per_sqm,\n" +
                " a.min_price,\n" +
                " a.max_price,\n" +
                " CAST(PERCENTILE_APPROX(CAST(b.price_per_sqm AS BIGINT), 0.5) AS INT) AS median_price,\n" +
                " CAST(VARIANCE(CAST(b.price_per_sqm AS DOUBLE)) AS INT) AS price_variance,\n" +
                " CAST(STDDEV(CAST(b.price_per_sqm AS DOUBLE)) AS INT) AS price_stddev,\n" +
                " a.avg_house_age,\n" +
                " '" + PARTITION_VALUE + "' AS load_date,\n" +
                " CASE\n" +
                " WHEN a.avg_price_per_sqm < 10000 THEN '低单价区'\n" +
                " WHEN a.avg_price_per_sqm < 30000 THEN '中单价区'\n" +
                " WHEN a.avg_price_per_sqm < 50000 THEN '高单价区'\n" +
                " ELSE '超高单价区'\n" +
                " END AS price_level,\n" +
                " CAST(ROUND(a.house_count * 100.0 / SUM(a.house_count) OVER (), 2) AS DECIMAL(5,2)) AS area_ratio,\n" +
                " " + ANALYSIS_CHECKID + " AS checkid\n" +
                "FROM agg_summary a\n" +
                "JOIN base_data b ON a.area_range = b.area_range\n" +
                "GROUP BY \n" +
                " a.area_range, a.house_count, a.avg_price_per_sqm, a.min_price, a.max_price, a.avg_house_age\n" +
                "ORDER BY \n" +
                " CASE a.area_range\n" +
                " WHEN '50㎡以下' THEN 1\n" +
                " WHEN '50-90㎡' THEN 2\n" +
                " WHEN '90-144㎡' THEN 3\n" +
                " WHEN '144-236㎡' THEN 4\n" +
                " ELSE 5\n" +
                " END";

        System.out.println("🚀 开始执行插入SQL（此操作可能需要数分钟，请耐心等待）...");
        System.out.println("💡 提示：如果集群资源紧张，YARN 任务启动可能较慢，但仍在运行中。");
        long start = System.currentTimeMillis();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            long elapsedSec = (System.currentTimeMillis() - start) / 1000;
            System.out.println("⏳ 插入操作仍在进行中... 已耗时 " + elapsedSec + " 秒");
        }, 30, 30, TimeUnit.SECONDS);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
            scheduler.shutdownNow();
            long duration = System.currentTimeMillis() - start;
            System.out.println("✅ 插入成功！总耗时: " + duration + " ms (" + (duration / 1000) + " 秒)");
        } catch (SQLException e) {
            scheduler.shutdownNow();
            System.err.println("💥 插入SQL执行失败！错误详情:");
            System.err.println("SQL: " + sql.substring(0, Math.min(200, sql.length())) + "...");
            throw e;
        }
    }

    private static void executeValidation(Connection conn) throws SQLException {
        String sql = String.format(
                "SELECT area_range, house_count, avg_price_per_sqm, price_level, checkid " +
                        "FROM %s.%s WHERE pt_date = '%s' AND checkid = %d " +
                        "ORDER BY avg_price_per_sqm DESC LIMIT 5",
                DATABASE, TARGET_TABLE, PARTITION_VALUE, ANALYSIS_CHECKID
        );
        System.out.println("🔍 正在验证写入结果...");
        try (PreparedStatement stmt = conn.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
            System.out.println("\n🔍 验证结果（前5条）:");
            System.out.println("面积区间\t数量\t均价\t等级\tcheckid");
            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                System.out.printf("%s\t%d\t%d\t%s\t%d%n",
                        rs.getString("area_range"),
                        rs.getInt("house_count"),
                        rs.getInt("avg_price_per_sqm"),
                        rs.getString("price_level"),
                        rs.getInt("checkid")
                );
            }
            if (!hasData) {
                System.out.println("⚠️ 未查到写入数据，请检查分区或条件是否正确。");
            }
        }
    }

    // ================= 新增方法：同步 Hive → MySQL =================
    private static void syncHiveToMysql(Connection hiveConn, Connection mysqlConn) throws SQLException {
        String selectSql = String.format(
                "SELECT " +
                        "area_range, house_count, avg_price_per_sqm, min_price, max_price, " +
                        "median_price, price_variance, price_stddev, avg_house_age, load_date, " +
                        "price_level, area_ratio, checkid, '%s' AS pt_date " +
                        "FROM %s.%s " +
                        "WHERE pt_date = '%s' AND checkid = %d",
                PARTITION_VALUE,
                DATABASE, TARGET_TABLE,
                PARTITION_VALUE, ANALYSIS_CHECKID
        );

        System.out.println("🔄 正在从 Hive 读取数据用于同步到 MySQL...");
        System.out.println("Executing: " + selectSql);

        List<Object[]> rows = new ArrayList<>();
        try (PreparedStatement hiveStmt = hiveConn.prepareStatement(selectSql);
             ResultSet rs = hiveStmt.executeQuery()) {

            while (rs.next()) {
                Object[] row = {
                        rs.getString("area_range"),
                        rs.getInt("house_count"),
                        rs.getObject("avg_price_per_sqm"), // 可能为 null
                        rs.getObject("min_price"),
                        rs.getObject("max_price"),
                        rs.getObject("median_price"),
                        rs.getObject("price_variance"),
                        rs.getObject("price_stddev"),
                        rs.getObject("avg_house_age"),
                        rs.getString("load_date"),
                        rs.getString("price_level"),
                        rs.getBigDecimal("area_ratio"),
                        rs.getInt("checkid"),
                        rs.getString("pt_date")
                };
                rows.add(row);
            }
        }

        if (rows.isEmpty()) {
            System.out.println("⚠️ Hive 中未找到待同步数据（pt_date='" + PARTITION_VALUE + "', checkid=" + ANALYSIS_CHECKID + "）");
            return;
        }

        System.out.println("📥 准备将 " + rows.size() + " 条记录写入 MySQL...");

        String insertSql = "INSERT INTO area_price_analysis (" +
                "area_range, house_count, avg_price_per_sqm, min_price, max_price, " +
                "median_price, price_variance, price_stddev, avg_house_age, load_date, " +
                "price_level, area_ratio, checkid, pt_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
}