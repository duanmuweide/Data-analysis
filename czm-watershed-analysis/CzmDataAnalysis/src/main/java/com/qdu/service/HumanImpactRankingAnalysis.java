package com.qdu.service;

import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;

import java.sql.*;

/**
 * 人类活动影响排名分析（2015年后 Top20/年）
 * 复用统一连接工具类，仅处理最新批次（maxId）数据
 * 结果导入MySQL：american_data_analysis数据库（新增hid列存储批次ID）
 */
public class HumanImpactRankingAnalysis {
  // 仅保留业务配置，连接配置移到工具类
  private static final String MYSQL_TARGET_TABLE = "human_impact_ranking_analysis";
  private static final int BATCH_SIZE = 100; // 批量提交大小

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

      // 3. 构建核心SQL（新增id字段 + 动态maxId）
      String hiveSql = "SELECT " +
              "    id,  " +
              "    year, " +
              "    FIPS, " +
              "    human_impact_score, " +
              "    impact_rank " +
              "FROM ( " +
              "    SELECT " +
              "        id,   " +
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
              "    WHERE id = " + maxId + " " +  // 动态传入最大批次ID
              "      AND year >= 2015 " +
              "      AND pmod(hash(FIPS), 8) BETWEEN 6 AND 8 " +
              "      AND n_human_food_dem_kgsqkm IS NOT NULL " +
              ") ranked " +
              "WHERE impact_rank <= 20 " +
              "ORDER BY year, impact_rank";

      // 4. 预编译MySQL插入语句（新增hid列，放在最后）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, fips, human_impact_score, impact_rank, hid) " +  // 新增hid列
                      "VALUES (?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 5. 执行Hive查询
      rs = hiveStmt.executeQuery(hiveSql);

      // 6. 遍历结果集并批量插入MySQL（新增hid参数）
      int batchCount = 0;
      while (rs.next()) {
        // 读取Hive返回的字段（新增id字段）
        int hid = rs.getInt("id");  // 批次ID -> MySQL的hid列
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        // 限制数值范围，避免超出MySQL DECIMAL(10,2)上限
        double humanImpactScore = Math.min(rs.getDouble("human_impact_score"), 99999999.99);
        int impactRank = rs.getInt("impact_rank");

        // 设置插入参数（最后一个参数为hid）
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setString(2, fips);
        mysqlPstmt.setDouble(3, humanImpactScore);
        mysqlPstmt.setInt(4, impactRank);
        mysqlPstmt.setInt(5, hid);  // 新增：设置hid参数

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          MysqlConnUtil.commit(mysqlConn);  // 复用工具类提交方法
          System.out.printf("已批量插入 %d 条人类活动影响排名数据（批次ID：%d）%n", batchCount, maxId);
        }
      }

      // 处理剩余数据
      if (batchCount > 0 && batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        MysqlConnUtil.commit(mysqlConn);
        System.out.printf("最终提交剩余数据，总计插入 %d 条数据（批次ID：%d）%n", batchCount, maxId);
      }

      System.out.printf("人类活动影响排名数据已成功导入MySQL！（本次批次ID：%d）%n", maxId);

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