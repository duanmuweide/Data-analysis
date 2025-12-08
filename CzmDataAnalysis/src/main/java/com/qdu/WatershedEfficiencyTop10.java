package com.qdu;

import java.sql.*;
import java.util.Properties;

public class WatershedEfficiencyTop10 {
  // Hive连接配置（需替换为实际环境）
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  // MySQL连接配置（需替换为实际环境）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376";

  // MySQL目标表名（需提前创建，表结构见下方说明）
  private static final String MYSQL_TARGET_TABLE = "watershed_efficiency_top10";

  public static void main(String[] args) {
    Connection hiveConn = null;
    Connection mysqlConn = null;
    Statement hiveStmt = null;
    PreparedStatement mysqlPstmt = null;
    ResultSet rs = null;

    try {
      // 1. 加载驱动（MySQL驱动5.1+可自动加载，Hive驱动需显式加载）
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Class.forName("com.mysql.cj.jdbc.Driver");

      // 2. 建立Hive连接
      Properties hiveProps = new Properties();
      hiveProps.setProperty("user", HIVE_USER);
      hiveProps.setProperty("password", HIVE_PASSWORD);
      hiveConn = DriverManager.getConnection(HIVE_URL, hiveProps);

      // 3. 建立MySQL连接
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
      // 关闭自动提交，开启批量插入优化
      mysqlConn.setAutoCommit(false);

      // 4. 构建Hive查询SQL（保持原逻辑）
      String hiveSql = "SELECT " +
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

      // 5. 预编译MySQL插入语句（推荐先清空表或根据业务逻辑处理重复数据）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, fips, n_efficiency, p_efficiency, avg_efficiency, ranks) " +
                      "VALUES (?, ?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 6. 执行Hive查询
      hiveStmt = hiveConn.createStatement();
      rs = hiveStmt.executeQuery(hiveSql);

      // 7. 遍历结果集并插入MySQL
      int batchCount = 0;
      final int BATCH_SIZE = 100; // 批量提交大小，可根据数据量调整
      while (rs.next()) {
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        double nEfficiency = rs.getDouble("n_efficiency");
        double pEfficiency = rs.getDouble("p_efficiency");
        double avgEfficiency = rs.getDouble("avg_efficiency");
        int rank = rs.getInt("rank");

        // 设置参数
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setString(2, fips);
        mysqlPstmt.setDouble(3, nEfficiency);
        mysqlPstmt.setDouble(4, pEfficiency);
        mysqlPstmt.setDouble(5, avgEfficiency);
        mysqlPstmt.setInt(6, rank);

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小则提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          mysqlConn.commit();
          System.out.printf("已批量插入 %d 条数据%n", batchCount);
        }
      }

      // 处理剩余数据
      if (batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        mysqlConn.commit();
        System.out.printf("最终提交剩余数据，总计插入 %d 条数据%n", batchCount);
      }

      System.out.println("数据全部导入MySQL完成！");

    } catch (ClassNotFoundException e) {
      System.err.println("驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    } catch (SQLException e) {
      System.err.println("数据库操作异常：" + e.getMessage());
      e.printStackTrace();
      // 回滚MySQL事务
      try {
        if (mysqlConn != null) {
          mysqlConn.rollback();
          System.out.println("MySQL事务已回滚");
        }
      } catch (SQLException rollbackEx) {
        System.err.println("事务回滚失败：" + rollbackEx.getMessage());
      }
    } finally {
      // 8. 关闭所有资源（逆序关闭）
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