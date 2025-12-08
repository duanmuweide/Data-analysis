package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 分析历史氮盈余与当前氮排放的相关性
 * 连接配置：master-pc:10000/data_analysis，用户名master，密码为空
 * 结果导入MySQL：american_data_analysis数据库
 */
public class NitrogenSurplusEmissionAnalysis {
  // 固定Hive连接配置
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = ""; // 密码为空

  // MySQL连接配置（适配你的环境）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376"; // 密码为空
  // MySQL目标表名
  private static final String MYSQL_TARGET_TABLE = "nitrogen_surplus_emission_analysis";

  public static void main(String[] args) {
    Connection hiveConn = null;
    Connection mysqlConn = null;
    Statement hiveStmt = null;
    PreparedStatement mysqlPstmt = null;
    ResultSet rs = null;

    try {
      // 1. 加载驱动
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Class.forName("com.mysql.cj.jdbc.Driver");

      // 2. 建立Hive连接
      Properties hiveProps = new Properties();
      hiveProps.setProperty("user", HIVE_USER);
      hiveProps.setProperty("password", HIVE_PASSWORD);
      hiveConn = DriverManager.getConnection(HIVE_URL, hiveProps);
      hiveStmt = hiveConn.createStatement();

      // 3. 建立MySQL连接
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
      mysqlConn.setAutoCommit(false); // 关闭自动提交，开启批量插入

      // 4. 构建核心SQL（保留原CTE逻辑）
      String hiveSql = "WITH cumulative_data AS ( " +
              "    SELECT " +
              "        FIPS, " +
              "        SUM(n_leg_ag_surplus_kgsqkm) as cumulative_surplus, " +
              "        AVG(n_leg_ag_surplus_kgsqkm) as avg_annual_surplus " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE year BETWEEN 1950 AND 2000 " +
              "    GROUP BY FIPS " +
              "), " +
              "emission_data AS ( " +
              "    SELECT " +
              "        FIPS, " +
              "        AVG(n_emis_total_kgsqkm) as avg_annual_emission " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE year >= 2001 " +
              "    GROUP BY FIPS " +
              ") " +
              "SELECT " +
              "    c.FIPS, " +
              "    c.cumulative_surplus, " +
              "    c.avg_annual_surplus, " +
              "    e.avg_annual_emission, " +
              "    ROUND(e.avg_annual_emission / NULLIF(c.avg_annual_surplus, 0), 3) as emission_per_surplus_ratio, " +
              "    CASE " +
              "        WHEN e.avg_annual_emission > 2000 THEN '严重排放' " +
              "        WHEN e.avg_annual_emission > 1000 THEN '高排放' " +
              "        WHEN e.avg_annual_emission > 500 THEN '中排放' " +
              "        ELSE '低排放' " +
              "    END as emission_level " +
              "FROM cumulative_data c " +
              "JOIN emission_data e ON c.FIPS = e.FIPS " +
              "WHERE c.cumulative_surplus > 0 " +
              "ORDER BY emission_per_surplus_ratio DESC " +
              "LIMIT 50";

      // 5. 预编译MySQL插入语句（字段无关键字冲突）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (fips, cumulative_surplus, avg_annual_surplus, avg_annual_emission, " +
                      "emission_per_surplus_ratio, emission_level) " +
                      "VALUES (?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 6. 执行Hive查询
      rs = hiveStmt.executeQuery(hiveSql);

      // 7. 遍历结果集并批量插入MySQL
      int batchCount = 0;
      final int BATCH_SIZE = 50; // Top50数据，批量大小设为50即可
      while (rs.next()) {
        String fips = rs.getString("FIPS");
        // 限制数值范围，避免超出MySQL DECIMAL(18,3)上限
        double cumulativeSurplus = Math.min(rs.getDouble("cumulative_surplus"), 9999999999999999.999);
        double avgAnnualSurplus = Math.min(rs.getDouble("avg_annual_surplus"), 9999999999999999.999);
        double avgAnnualEmission = Math.min(rs.getDouble("avg_annual_emission"), 9999999999999999.999);
        double emissionRatio = rs.getDouble("emission_per_surplus_ratio");
        String emissionLevel = rs.getString("emission_level");

        // 设置插入参数
        mysqlPstmt.setString(1, fips);
        mysqlPstmt.setDouble(2, cumulativeSurplus);
        mysqlPstmt.setDouble(3, avgAnnualSurplus);
        mysqlPstmt.setDouble(4, avgAnnualEmission);
        mysqlPstmt.setDouble(5, emissionRatio);
        mysqlPstmt.setString(6, emissionLevel);

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;
      }

      // 执行批量插入（Top50数据一次性提交）
      if (batchCount > 0) {
        mysqlPstmt.executeBatch();
        mysqlConn.commit();
        System.out.printf("成功插入 %d 条氮盈余与排放相关性数据到MySQL%n", batchCount);
      } else {
        System.out.println("Hive查询结果为空，无数据插入");
      }

    } catch (ClassNotFoundException e) {
      System.err.println("驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
      e.printStackTrace();
      // 事务回滚
      try {
        if (mysqlConn != null) {
          mysqlConn.rollback();
          System.out.println("MySQL事务已回滚");
        }
      } catch (SQLException rollbackEx) {
        System.err.println("事务回滚失败：" + rollbackEx.getMessage());
      }
    } finally {
      // 8. 关闭所有资源
      closeResources(rs, hiveStmt, hiveConn);
      try {
        if (mysqlPstmt != null) mysqlPstmt.close();
        if (mysqlConn != null) mysqlConn.close();
      } catch (SQLException e) {
        System.err.println("MySQL资源关闭异常：" + e.getMessage());
      }
      System.out.println("所有数据库连接已关闭");
    }
  }

  /**
   * 统一关闭JDBC资源
   */
  private static void closeResources(ResultSet rs, Statement stmt, Connection conn) {
    try {
      if (rs != null) rs.close();
      if (stmt != null) stmt.close();
      if (conn != null) conn.close();
    } catch (SQLException e) {
      System.err.println("Hive资源关闭异常：" + e.getMessage());
    }
  }
}