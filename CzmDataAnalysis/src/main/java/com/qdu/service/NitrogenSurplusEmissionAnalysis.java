package com.qdu.service;

import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;

import java.sql.*;

/**
 * 分析历史氮盈余与当前氮排放的相关性
 * 复用统一连接工具类，仅处理最新批次（maxId）数据
 * 结果导入MySQL：american_data_analysis数据库（新增hid列存储批次ID）
 */
public class NitrogenSurplusEmissionAnalysis {
  // 仅保留业务配置，连接配置移到工具类
  private static final String MYSQL_TARGET_TABLE = "nitrogen_surplus_emission_analysis";
  private static final int BATCH_SIZE = 50; // Top50数据，批量大小设为50

  public static void main(String[] args) throws SQLException {
    Connection hiveConn = null;
    Connection mysqlConn = null;
    Statement hiveStmt = null;
    PreparedStatement mysqlPstmt = null;
    ResultSet rs = null;
    int maxId = 0; // 存储最大批次ID

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
      hiveStmt = hiveConn.createStatement();

      // 3. 构建核心SQL（新增id字段 + 动态maxId + 关联id）
      String hiveSql = "WITH cumulative_data AS ( " +
              "    SELECT " +
              "        id,  " +
              "        FIPS, " +
              "        SUM(n_leg_ag_surplus_kgsqkm) as cumulative_surplus, " +
              "        AVG(n_leg_ag_surplus_kgsqkm) as avg_annual_surplus " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE id = " + maxId + " " +  // 动态传入最大批次ID
              "    AND year BETWEEN 1950 AND 2000 " +
              "    GROUP BY id, FIPS " +  // GROUP BY新增id
              "), " +
              "emission_data AS ( " +
              "    SELECT " +
              "        id,  " +
              "        FIPS, " +
              "        AVG(n_emis_total_kgsqkm) as avg_annual_emission " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE id = " + maxId + " " +  // 动态传入最大批次ID
              "    AND year >= 2001 " +
              "    GROUP BY id, FIPS " +  // GROUP BY新增id
              ") " +
              "SELECT " +
              "    c.id,  " +
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
              "JOIN emission_data e ON c.FIPS = e.FIPS AND c.id = e.id " +  // 关联条件新增id
              "WHERE c.cumulative_surplus > 0 " +
              "ORDER BY emission_per_surplus_ratio DESC " +
              "LIMIT 50";

      // 4. 预编译MySQL插入语句（新增hid列，放在最后）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (fips, cumulative_surplus, avg_annual_surplus, avg_annual_emission, " +
                      "emission_per_surplus_ratio, emission_level, hid) " +  // 新增hid列
                      "VALUES (?, ?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 5. 执行Hive查询
      rs = hiveStmt.executeQuery(hiveSql);

      // 6. 遍历结果集并批量插入MySQL（新增hid参数）
      int batchCount = 0;
      while (rs.next()) {
        // 读取Hive返回的字段（新增id字段）
        int hid = rs.getInt("id");  // 批次ID -> MySQL的hid列
        String fips = rs.getString("FIPS");
        // 限制数值范围，避免超出MySQL DECIMAL(20,3)上限
        double cumulativeSurplus = Math.min(rs.getDouble("cumulative_surplus"), 9999999999999999.999);
        double avgAnnualSurplus = Math.min(rs.getDouble("avg_annual_surplus"), 9999999999999999.999);
        double avgAnnualEmission = Math.min(rs.getDouble("avg_annual_emission"), 9999999999999999.999);
        double emissionRatio = rs.getDouble("emission_per_surplus_ratio");
        String emissionLevel = rs.getString("emission_level");

        // 设置插入参数（最后一个参数为hid）
        mysqlPstmt.setString(1, fips);
        mysqlPstmt.setDouble(2, cumulativeSurplus);
        mysqlPstmt.setDouble(3, avgAnnualSurplus);
        mysqlPstmt.setDouble(4, avgAnnualEmission);
        mysqlPstmt.setDouble(5, emissionRatio);
        mysqlPstmt.setString(6, emissionLevel);
        mysqlPstmt.setInt(7, hid);  // 新增：设置hid参数

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;
      }

      // 执行批量插入（Top50数据一次性提交）
      if (batchCount > 0) {
        mysqlPstmt.executeBatch();
        MysqlConnUtil.commit(mysqlConn);  // 复用工具类提交方法
        System.out.printf("成功插入 %d 条氮盈余与排放相关性数据到MySQL（批次ID：%d）%n", batchCount, maxId);
      } else {
        System.out.println("Hive查询结果为空，无数据插入（批次ID：" + maxId + "）");
      }

    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
      e.printStackTrace();
      // 复用工具类回滚方法
      MysqlConnUtil.rollback(mysqlConn);
    } finally {
      // 关闭所有资源（复用工具类关闭方法）
      try {
        if (rs != null) rs.close();
        HiveConnUtil.closeConnection(hiveConn, hiveStmt);
        MysqlConnUtil.closeConnection(mysqlConn, mysqlPstmt);
      } catch (SQLException e) {
        System.err.println("资源关闭异常：" + e.getMessage());
      }
      System.out.println("所有数据库连接已关闭");
    }
  }
}