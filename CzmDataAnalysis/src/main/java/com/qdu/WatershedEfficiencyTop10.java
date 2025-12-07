package com.qdu;

import java.sql.*;
import java.util.Properties;

public class WatershedEfficiencyTop10 {
  // Hive连接配置（需替换为实际环境）
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  public static void main(String[] args) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      // 1. 加载Hive JDBC驱动
      Class.forName("org.apache.hive.jdbc.HiveDriver");

      // 2. 创建连接
      Properties props = new Properties();
      props.setProperty("user", HIVE_USER);
      props.setProperty("password", HIVE_PASSWORD);
      conn = DriverManager.getConnection(HIVE_URL, props);

      // 3. 构建查询SQL（保持原逻辑）
      String sql = "SELECT " +
              "year, " +
              "FIPS, " +
              "ROUND(n_nue_kgsqkm, 3) as n_efficiency, " +
              "ROUND(p_nue_kgsqkm, 3) as p_efficiency, " +
              "ROUND((n_nue_kgsqkm + p_nue_kgsqkm) / 2, 3) as avg_efficiency, " +
              "rank " +
              "FROM ( " +
              "    SELECT " +
              "        year, " +
              "        FIPS, " +
              "        n_nue_kgsqkm, " +
              "        p_nue_kgsqkm, " +
              "        ROW_NUMBER() OVER ( " +
              "            PARTITION BY year " +
              "            ORDER BY (n_nue_kgsqkm + p_nue_kgsqkm) / 2 DESC " +
              "        ) as rank " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE year >= 2000 " +
              "    AND n_nue_kgsqkm IS NOT NULL " +
              "    AND p_nue_kgsqkm IS NOT NULL " +
              "    AND n_nue_kgsqkm BETWEEN 0 AND 1 " +
              "    AND p_nue_kgsqkm BETWEEN 0 AND 1 " +
              ") t " +
              "WHERE rank <= 10 " +
              "ORDER BY year, rank";

      // 4. 执行查询
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      // 5. 遍历结果集（按需处理结果）
      System.out.println("year\tFIPS\tn_efficiency\tp_efficiency\tavg_efficiency\trank");
      while (rs.next()) {
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        double nEfficiency = rs.getDouble("n_efficiency");
        double pEfficiency = rs.getDouble("p_efficiency");
        double avgEfficiency = rs.getDouble("avg_efficiency");
        int rank = rs.getInt("rank");

        // 打印结果（可替换为入库/封装对象等逻辑）
        System.out.printf("%d\t%s\t%.3f\t%.3f\t%.3f\t%d%n",
                year, fips, nEfficiency, pEfficiency, avgEfficiency, rank);
      }

    } catch (ClassNotFoundException e) {
      System.err.println("Hive驱动加载失败：" + e.getMessage());
    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
    } finally {
      // 6. 关闭资源
      try {
        if (rs != null) rs.close();
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
      } catch (SQLException e) {
        System.err.println("资源关闭异常：" + e.getMessage());
      }
    }
  }
}