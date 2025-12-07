package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 分析历史氮盈余与当前氮排放的相关性
 * 连接配置：master-pc:10000/data_analysis，用户名master，密码为空
 */
public class NitrogenSurplusEmissionAnalysis {
  // 固定Hive连接配置
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = ""; // 密码为空

  public static void main(String[] args) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      // 1. 加载驱动并创建连接
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Properties props = new Properties();
      props.setProperty("user", HIVE_USER);
      props.setProperty("password", HIVE_PASSWORD);
      conn = DriverManager.getConnection(HIVE_URL, props);
      stmt = conn.createStatement();

      // 2. 构建核心SQL（保留原CTE逻辑）
      String sql = "WITH cumulative_data AS ( " +
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

      // 3. 执行查询并输出结果
      rs = stmt.executeQuery(sql);
      System.out.println("===== 历史氮盈余与当前氮排放相关性分析（Top50） =====");
      System.out.println("FIPS\t累计氮盈余\t年均氮盈余\t年均氮排放\t排放/盈余比\t排放等级");
      while (rs.next()) {
        String fips = rs.getString("FIPS");
        double cumulativeSurplus = rs.getDouble("cumulative_surplus");
        double avgAnnualSurplus = rs.getDouble("avg_annual_surplus");
        double avgAnnualEmission = rs.getDouble("avg_annual_emission");
        double emissionRatio = rs.getDouble("emission_per_surplus_ratio");
        String emissionLevel = rs.getString("emission_level");

        // 格式化输出，保留小数位提升可读性
        System.out.printf("%s\t%.2f\t%.2f\t%.2f\t%.3f\t%s%n",
                fips, cumulativeSurplus, avgAnnualSurplus, avgAnnualEmission, emissionRatio, emissionLevel);
      }

    } catch (ClassNotFoundException e) {
      System.err.println("Hive驱动加载失败：" + e.getMessage());
    } catch (SQLException e) {
      System.err.println("SQL执行异常：" + e.getMessage());
      e.printStackTrace();
    } finally {
      // 4. 关闭资源
      closeResources(rs, stmt, conn);
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
      System.err.println("资源关闭异常：" + e.getMessage());
    }
  }
}