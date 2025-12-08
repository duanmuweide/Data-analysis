package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 流域农业氮盈余与大气氮沉降相关性分析（含自定义UDF）
 */
public class NitrogenSurplusAtmDepCorr {
  // Hive连接参数
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  // UDF相关SQL
  private static final String ADD_JAR_SQL = "ADD JAR hdfs://master-pc:9870/user/hive/udf/CzmDataAnalysis-1.0-SNAPSHOT.jar;";
  private static final String CREATE_UDF_SQL =
          "CREATE TEMPORARY FUNCTION calc_corr_aux AS 'com.qdu.CorrCoefficientUDF';";

  // 核心查询SQL
  private static final String QUERY_SQL =
          "SELECT " +
                  "    year, " +
                  "    ROUND(AVG(n_ag_surplus_kgsqkm), 3) AS avg_n_ag_surplus, " +
                  "    ROUND(AVG(n_atm_dep_kgsqkm), 3) AS avg_n_atm_dep, " +
                  "    calc_corr_aux(COLLECT_LIST(n_ag_surplus_kgsqkm), COLLECT_LIST(n_atm_dep_kgsqkm)) AS corr_coefficient " +
                  "FROM watershed_nutrient_balance " +
                  "WHERE pmod(hash(FIPS), 8) BETWEEN 1 AND 4 " +
                  "  AND n_ag_surplus_kgsqkm IS NOT NULL " +
                  "  AND n_atm_dep_kgsqkm IS NOT NULL " +
                  "GROUP BY year " +
                  "ORDER BY year ASC";

  public static void main(String[] args) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    try {
      // 1. 加载驱动 + 创建连接
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Properties props = new Properties();
      props.setProperty("user", HIVE_USER);
      props.setProperty("password", HIVE_PASSWORD);
      conn = DriverManager.getConnection(HIVE_URL, props);
      stmt = conn.createStatement();

      // 2. 加载UDF JAR
      try {
        stmt.execute(ADD_JAR_SQL);
        System.out.println("UDF JAR加载成功！");
      } catch (SQLException e) {
        System.err.println("UDF JAR加载失败：" + e.getMessage());
        throw e;
      }

      // 3. 创建临时UDF
      try {
        stmt.execute(CREATE_UDF_SQL);
        System.out.println("自定义函数calc_corr_aux创建成功！");
      } catch (SQLException e) {
        if (e.getMessage().contains("Function 'calc_corr_aux' already exists")) {
          System.out.println("自定义函数已存在，跳过创建！");
        } else {
          throw e;
        }
      }

      // 4. 执行查询
      rs = stmt.executeQuery(QUERY_SQL);

      // 5. 输出结果
      System.out.println("\n===== 流域农业氮盈余与大气氮沉降相关性分析 =====");
      System.out.println("年份\t平均农业氮盈余(kg N/km²)\t平均大气氮沉降(kg N/km²)\t皮尔逊相关系数");
      while (rs.next()) {
        int year = rs.getInt("year");
        double avgSurplus = rs.getDouble("avg_n_ag_surplus");
        double avgAtmDep = rs.getDouble("avg_n_atm_dep");
        String corrCoeff = rs.getString("corr_coefficient");

        System.out.printf("%d\t%.3f\t\t\t%.3f\t\t\t%s%n",
                year, avgSurplus, avgAtmDep, corrCoeff);
      }

    } catch (ClassNotFoundException e) {
      System.err.println("Hive驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    } catch (SQLException e) {
      System.err.println("SQL执行异常：" + e.getMessage());
      e.printStackTrace();
    } finally {
      closeResources(rs, stmt, conn);
    }
  }

  /**
   * 关闭JDBC资源
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