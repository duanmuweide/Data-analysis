package com.qdu.service;

import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;

import java.sql.*;

public class PhosphorusPollutionByBasinArea {
  // 仅保留业务相关配置，连接配置全部移到工具类
  private static final String MYSQL_TARGET_TABLE = "phosphorus_pollution_by_basin_area";
  private static final int BATCH_SIZE = 50;  // 批量提交大小

  public static void main(String[] args) throws SQLException {
    // 声明数据库资源
    Connection hiveConn = null;
    Connection mysqlConn = null;
    Statement hiveStmt = null;
    PreparedStatement mysqlPstmt = null;
    ResultSet rs = null;
    int maxId = 0;  // 存储最大批次ID

    try {
      // 1. 获取本次上传的最大批次ID（复用工具类）
      maxId = HiveMaxIdQueryUtil.getMaxId();
      if (maxId == 0) {
        System.out.println("Hive表中无数据，程序退出！");
        return;
      }
      System.out.println("本次查询的批次ID：" + maxId);

      // 2. 获取数据库连接（完全复用工具类，无硬编码配置）
      hiveConn = HiveConnUtil.getHiveConnection();
      mysqlConn = MysqlConnUtil.getMysqlConnection();

      // 3. 构建HQL（新增id字段 + 动态maxId + GROUP BY添加id）
      String hiveSql = "SELECT " +
              "id, " +
              "CASE " +
              "    WHEN area_sqkm < 1000 THEN '小型流域(<1000km²)' " +
              "    WHEN area_sqkm BETWEEN 1000 AND 5000 THEN '中型流域(1000-5000km²)' " +
              "    ELSE '大型流域(>5000km²)' " +
              "END AS area_grade, " +
              "COUNT(DISTINCT FIPS) AS basin_count, " +
              "ROUND(SUM(p_farm_fert_kgsqkm), 2) AS farm_fert_total, " +
              "ROUND(SUM(p_manure_kgsqkm), 2) AS manure_total, " +
              "ROUND(SUM(p_point_source_kgsqkm), 2) AS point_source_total, " +
              "ROUND(SUM(p_farm_fert_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS farm_fert_ratio, " +
              "ROUND(SUM(p_manure_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS manure_ratio, " +
              "ROUND(SUM(p_point_source_kgsqkm) / SUM(p_farm_fert_kgsqkm + p_manure_kgsqkm + p_point_source_kgsqkm) * 100, 2) AS point_source_ratio " +
              "FROM watershed_nutrient_balance " +
              "WHERE id = " + maxId + " " +  // 动态传入最大批次ID
              "AND year BETWEEN 2010 AND 2020 " +
              "AND p_farm_fert_kgsqkm IS NOT NULL " +
              "AND p_manure_kgsqkm IS NOT NULL " +
              "AND p_point_source_kgsqkm IS NOT NULL " +
              "AND area_sqkm > 0 " +
              "GROUP BY " +
              "id," +
              "CASE " +
              "    WHEN area_sqkm < 1000 THEN '小型流域(<1000km²)' " +
              "    WHEN area_sqkm BETWEEN 1000 AND 5000 THEN '中型流域(1000-5000km²)' " +
              "    ELSE '大型流域(>5000km²)' " +
              "END " +
              "ORDER BY basin_count DESC";

      // 4. 预编译MySQL插入语句（新增hid列，放在最后）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (area_grade, basin_count, farm_fert_total, manure_total, " +
                      "point_source_total, farm_fert_ratio, manure_ratio, point_source_ratio, hid) " +  // 新增hid列
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 5. 执行Hive查询
      hiveStmt = hiveConn.createStatement();
      rs = hiveStmt.executeQuery(hiveSql);

      // 6. 遍历结果集并批量插入MySQL（新增hid参数）
      int batchCount = 0;
      while (rs.next()) {
        // 读取Hive返回的字段（新增id字段）
        int hid = rs.getInt("id");  // 批次ID -> MySQL的hid列
        String areaGrade = rs.getString("area_grade");
        int basinCount = rs.getInt("basin_count");
        double farmFertTotal = rs.getDouble("farm_fert_total");
        double manureTotal = rs.getDouble("manure_total");
        double pointSourceTotal = rs.getDouble("point_source_total");
        double farmFertRatio = rs.getDouble("farm_fert_ratio");
        double manureRatio = rs.getDouble("manure_ratio");
        double pointSourceRatio = rs.getDouble("point_source_ratio");

        // 设置插入参数（最后一个参数为hid）
        mysqlPstmt.setString(1, areaGrade);
        mysqlPstmt.setInt(2, basinCount);
        mysqlPstmt.setDouble(3, farmFertTotal);
        mysqlPstmt.setDouble(4, manureTotal);
        mysqlPstmt.setDouble(5, pointSourceTotal);
        mysqlPstmt.setDouble(6, farmFertRatio);
        mysqlPstmt.setDouble(7, manureRatio);
        mysqlPstmt.setDouble(8, pointSourceRatio);
        mysqlPstmt.setInt(9, hid);  // 新增：设置hid参数

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          MysqlConnUtil.commit(mysqlConn);  // 复用工具类提交方法
          System.out.printf("已批量插入 %d 条数据（批次ID：%d）%n", batchCount, maxId);
        }
      }

      // 处理剩余数据
      if (batchCount > 0 && batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        MysqlConnUtil.commit(mysqlConn);
        System.out.printf("最终提交剩余数据，总计插入 %d 条数据（批次ID：%d）%n", batchCount, maxId);
      }

      System.out.printf("磷污染数据已成功导入MySQL！（本次批次ID：%d）%n", maxId);

    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
      e.printStackTrace();
      // 复用工具类回滚方法
      MysqlConnUtil.rollback(mysqlConn);
    } finally {
      // 关闭所有资源（复用工具类关闭方法）
      try {
        if (rs != null) rs.close();
        MysqlConnUtil.closeConnection(mysqlConn, mysqlPstmt);
        HiveConnUtil.closeConnection(hiveConn, hiveStmt);
        System.out.println("数据库连接已全部关闭");
      } catch (SQLException e) {
        System.err.println("资源关闭异常：" + e.getMessage());
      }
    }
  }
}