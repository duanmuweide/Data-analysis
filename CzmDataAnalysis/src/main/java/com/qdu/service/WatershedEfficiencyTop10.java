package com.qdu.service;

import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;

import java.sql.*;

public class WatershedEfficiencyTop10 {
  // 仅保留业务相关配置，连接配置全部移到工具类
  private static final String MYSQL_TARGET_TABLE = "watershed_efficiency_top10";
  private static final int BATCH_SIZE = 100;  // 批量提交大小

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

      // 3. 构建HQL（新增id字段 + 动态maxId）
      String hiveSql = "SELECT " +
              "id, " +  // Hive批次ID
              "year, " +
              "FIPS, " +
              "ROUND(n_nue_kgsqkm, 3) as n_efficiency, " +
              "ROUND(p_nue_kgsqkm, 3) as p_efficiency, " +
              "ROUND((n_nue_kgsqkm + p_nue_kgsqkm) / 2, 3) as avg_efficiency, " +
              "rank " +
              "FROM ( " +
              "    SELECT " +
              "        id, " +
              "        year, " +
              "        FIPS, " +
              "        n_nue_kgsqkm, " +
              "        p_nue_kgsqkm, " +
              "        ROW_NUMBER() OVER ( " +
              "            PARTITION BY year " +
              "            ORDER BY (n_nue_kgsqkm + p_nue_kgsqkm) / 2 DESC " +
              "        ) as rank " +
              "    FROM watershed_nutrient_balance " +
              "    WHERE id = " + maxId + " " +  // 动态传入最大批次ID!
              "    AND year >= 2000 " +
              "    AND n_nue_kgsqkm IS NOT NULL " +
              "    AND p_nue_kgsqkm IS NOT NULL " +
              "    AND n_nue_kgsqkm BETWEEN 0 AND 1 " +
              "    AND p_nue_kgsqkm BETWEEN 0 AND 1 " +
              ") t " +
              "WHERE rank <= 10 " +
              "ORDER BY year, rank";

      // 4. 预编译MySQL插入语句（适配新增的hid列）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, fips, n_efficiency, p_efficiency, avg_efficiency, ranks, hid) " +
                      "VALUES (?, ?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 5. 执行Hive查询
      hiveStmt = hiveConn.createStatement();
      rs = hiveStmt.executeQuery(hiveSql);

      // 6. 遍历结果集插入MySQL（hid对应Hive的id）
      int batchCount = 0;
      while (rs.next()) {
        int hid = rs.getInt("id");          // Hive批次ID -> MySQL的hid列
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        double nEfficiency = rs.getDouble("n_efficiency");
        double pEfficiency = rs.getDouble("p_efficiency");
        double avgEfficiency = rs.getDouble("avg_efficiency");
        int rank = rs.getInt("rank");

        // 设置参数（hid对应第一个占位符）
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setString(2, fips);
        mysqlPstmt.setDouble(3, nEfficiency);
        mysqlPstmt.setDouble(4, pEfficiency);
        mysqlPstmt.setDouble(5, avgEfficiency);
        mysqlPstmt.setInt(6, rank);
        mysqlPstmt.setInt(7, hid);

        // 批量添加
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          MysqlConnUtil.commit(mysqlConn);  // 复用工具类的提交方法
          System.out.println("已批量插入：" + batchCount + " 条数据");
        }
      }

      // 处理剩余数据
      if (batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        MysqlConnUtil.commit(mysqlConn);
        System.out.println("最终提交剩余数据，总计插入：" + batchCount + " 条数据");
      }

      System.out.println("数据全部导入MySQL完成！");

    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
      e.printStackTrace();
      MysqlConnUtil.rollback(mysqlConn);  // 复用工具类的回滚方法
    } finally {
      // 关闭资源（复用工具类的关闭方法）
      try {
        if (rs != null) rs.close();
        MysqlConnUtil.closeConnection(mysqlConn, mysqlPstmt);
        HiveConnUtil.closeConnection(hiveConn, hiveStmt);
        System.out.println("所有连接已关闭");
      } catch (SQLException e) {
        System.err.println("关闭资源失败：" + e.getMessage());
      }
    }
  }
}