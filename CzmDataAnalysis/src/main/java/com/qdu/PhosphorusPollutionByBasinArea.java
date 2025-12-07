package com.qdu;

import java.sql.*;
import java.util.Properties;

public class PhosphorusPollutionByBasinArea {
  // 替换为实际Hive集群配置
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

      // 2. 创建Hive连接
      Properties props = new Properties();
      props.setProperty("user", HIVE_USER);
      props.setProperty("password", HIVE_PASSWORD);
      conn = DriverManager.getConnection(HIVE_URL, props);

      // 3. 构建统计SQL（完整保留原查询逻辑）
      String sql = "SELECT " +
              "    CASE " +
              "        WHEN area_sqkm < 1000 THEN '小型流域(<1000km²)' " +
              "        WHEN area_sqkm BETWEEN 1000 AND 5000 THEN '中型流域(1000-5000km²)' " +
              "        ELSE '大型流域(>5000km²)' " +
              "    END AS area_grade, " +
              "    COUNT(DISTINCT FIPS) AS basin_count, " +
              "    ROUND(SUM(p_farm_fert_kgsqkm), 2) AS farm_fert_total, " +
              "    ROUND(SUM(p_manure_kgsqkm), 2) AS manure_total, " +
              "    ROUND(SUM(p_point_source_kgsqkm), 2) AS point_source_total, " +
              "    ROUND(SUM(p_farm_fert_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS farm_fert_ratio, " +
              "    ROUND(SUM(p_manure_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS manure_ratio, " +
              "    ROUND(SUM(p_point_source_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS point_source_ratio " +
              "FROM watershed_nutrient_balance " +
              "WHERE year BETWEEN 2010 AND 2020 " +
              "AND p_farm_fert_kgsqkm IS NOT NULL " +
              "AND p_manure_kgsqkm IS NOT NULL " +
              "AND p_point_source_kgsqkm IS NOT NULL " +
              "AND area_sqkm > 0 " +
              "GROUP BY " +
              "    CASE " +
              "        WHEN area_sqkm < 1000 THEN '小型流域(<1000km²)' " +
              "        WHEN area_sqkm BETWEEN 1000 AND 5000 THEN '中型流域(1000-5000km²)' " +
              "        ELSE '大型流域(>5000km²)' " +
              "    END " +
              "ORDER BY basin_count DESC";

      // 4. 执行查询
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);

      // 5. 打印结果表头
      System.out.println("流域面积等级\t流域数量\t农业磷肥总量\t畜禽粪便磷总量\t点源磷总量\t磷肥占比(%)\t粪便磷占比(%)\t点源磷占比(%)");
      // 6. 遍历结果集
      while (rs.next()) {
        String areaGrade = rs.getString("area_grade");
        int basinCount = rs.getInt("basin_count");
        double farmFertTotal = rs.getDouble("farm_fert_total");
        double manureTotal = rs.getDouble("manure_total");
        double pointSourceTotal = rs.getDouble("point_source_total");
        double farmFertRatio = rs.getDouble("farm_fert_ratio");
        double manureRatio = rs.getDouble("manure_ratio");
        double pointSourceRatio = rs.getDouble("point_source_ratio");

        // 格式化输出结果
        System.out.printf("%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f%n",
                areaGrade, basinCount, farmFertTotal, manureTotal, pointSourceTotal,
                farmFertRatio, manureRatio, pointSourceRatio);
      }

    } catch (ClassNotFoundException e) {
      System.err.println("Hive JDBC驱动加载失败：" + e.getMessage());
    } catch (SQLException e) {
      System.err.println("Hive查询执行异常：" + e.getMessage());
      e.printStackTrace(); // 打印完整异常栈，便于定位SQL/连接问题
    } finally {
      // 7. 关闭资源
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