package com.qdu;

import java.sql.*;
import java.util.Properties;

public class PhosphorusPollutionByBasinArea {
  // Hive连接配置（保持原配置）
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  // MySQL连接配置（适配你的环境：数据库名american_data_analysis，密码为空）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376"; // 密码为空
  // MySQL目标表名（需提前创建，表结构见下方）
  private static final String MYSQL_TARGET_TABLE = "phosphorus_pollution_by_basin_area";

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

      // 3. 建立MySQL连接（密码为空）
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
      mysqlConn.setAutoCommit(false); // 关闭自动提交，开启批量插入

      // 4. 构建Hive查询SQL（保留原逻辑）
      String hiveSql = "SELECT " +
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

      // 5. 预编译MySQL插入语句（字段名无关键字冲突，无需反引号）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (area_grade, basin_count, farm_fert_total, manure_total, " +
                      "point_source_total, farm_fert_ratio, manure_ratio, point_source_ratio) " +
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 6. 执行Hive查询
      hiveStmt = hiveConn.createStatement();
      rs = hiveStmt.executeQuery(hiveSql);

      // 7. 遍历结果集并批量插入MySQL
      int batchCount = 0;
      final int BATCH_SIZE = 50; // 批量提交大小（数据量小，设为50即可）
      while (rs.next()) {
        String areaGrade = rs.getString("area_grade");
        int basinCount = rs.getInt("basin_count");
        double farmFertTotal = rs.getDouble("farm_fert_total");
        double manureTotal = rs.getDouble("manure_total");
        double pointSourceTotal = rs.getDouble("point_source_total");
        double farmFertRatio = rs.getDouble("farm_fert_ratio");
        double manureRatio = rs.getDouble("manure_ratio");
        double pointSourceRatio = rs.getDouble("point_source_ratio");

        // 设置插入参数
        mysqlPstmt.setString(1, areaGrade);
        mysqlPstmt.setInt(2, basinCount);
        mysqlPstmt.setDouble(3, farmFertTotal);
        mysqlPstmt.setDouble(4, manureTotal);
        mysqlPstmt.setDouble(5, pointSourceTotal);
        mysqlPstmt.setDouble(6, farmFertRatio);
        mysqlPstmt.setDouble(7, manureRatio);
        mysqlPstmt.setDouble(8, pointSourceRatio);

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          mysqlConn.commit();
          System.out.printf("已批量插入 %d 条数据%n", batchCount);
        }
      }

      // 处理剩余数据
      if (batchCount > 0 && batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        mysqlConn.commit();
        System.out.printf("最终提交剩余数据，总计插入 %d 条数据%n", batchCount);
      }

      System.out.println("磷污染数据已成功导入MySQL（american_data_analysis数据库）！");

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
      // 关闭所有资源
      try {
        if (rs != null) rs.close();
        if (mysqlPstmt != null) mysqlPstmt.close();
        if (hiveStmt != null) hiveStmt.close();
        if (mysqlConn != null) mysqlConn.close();
        if (hiveConn != null) hiveConn.close();
        System.out.println("数据库连接已全部关闭");
      } catch (SQLException e) {
        System.err.println("资源关闭异常：" + e.getMessage());
      }
    }
  }
}