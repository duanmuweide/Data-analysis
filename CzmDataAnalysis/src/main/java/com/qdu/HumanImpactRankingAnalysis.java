package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 人类活动影响排名分析（2015年后 Top20/年）
 * 连接配置：master-pc:10000/data_analysis，用户名master，密码为空
 */
public class HumanImpactRankingAnalysis {
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

      // 2. 构建核心SQL（保留原排名和评分逻辑）
      String sql = "SELECT " +
              "    year, " +
              "    FIPS, " +
              "    human_impact_score, " +
              "    impact_rank " +
              "FROM ( " +
              "    SELECT " +
              "        year, " +
              "        FIPS, " +
              "        ROUND( " +
              "            (COALESCE(n_human_food_dem_kgsqkm, 0) / 100) * 30 + " +
              "            (COALESCE(p_human_nonfood_dem_kgsqkm, 0) / 50) * 25 + " +
              "            (COALESCE(n_human_food_waste_kgsqkm, 0) / 80) * 45, " +
              "            2 " +
              "        ) AS human_impact_score, " +
              "        RANK() OVER (PARTITION BY year ORDER BY " +
              "            (COALESCE(n_human_food_dem_kgsqkm, 0) / 100) * 30 + " +
              "            (COALESCE(p_human_nonfood_dem_kgsqkm, 0) / 50) * 25 + " +
              "            (COALESCE(n_human_food_waste_kgsqkm, 0) / 80) * 45 DESC " +
              "        ) AS impact_rank " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE year >= 2015 " +
              "      AND pmod(hash(FIPS), 8) BETWEEN 6 AND 8 " +
              "      AND n_human_food_dem_kgsqkm IS NOT NULL " +
              ") ranked " +
              "WHERE impact_rank <= 20 " +
              "ORDER BY year, impact_rank";

      // 3. 执行查询并输出结果
      rs = stmt.executeQuery(sql);
      System.out.println("===== 人类活动影响排名（2015年后 Top20/年） =====");
      System.out.println("年份\tFIPS\t人类活动影响得分\t排名");
      while (rs.next()) {
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        double humanImpactScore = rs.getDouble("human_impact_score");
        int impactRank = rs.getInt("impact_rank");

        System.out.printf("%d\t%s\t%.2f\t%d%n",
                year, fips, humanImpactScore, impactRank);
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
