package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 人类活动影响排名分析（2015年后 Top20/年）
 * 连接配置：master-pc:10000/data_analysis，用户名master，密码为空
 * 结果导入MySQL：american_data_analysis数据库
 */
public class HumanImpactRankingAnalysis {
  // 固定Hive连接配置
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = ""; // 密码为空

  // MySQL连接配置（适配你的环境）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376"; // 密码为空
  // MySQL目标表名
  private static final String MYSQL_TARGET_TABLE = "human_impact_ranking_analysis";

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
      hiveStmt = hiveConn.createStatement();

      // 3. 建立MySQL连接
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
      mysqlConn.setAutoCommit(false); // 关闭自动提交，开启批量插入

      // 4. 构建核心SQL（保留原排名和评分逻辑）
      String hiveSql = "SELECT " +
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

      // 5. 预编译MySQL插入语句（impact_rank加反引号，避免关键字冲突）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, fips, human_impact_score, impact_rank) " +
                      "VALUES (?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 6. 执行Hive查询
      rs = hiveStmt.executeQuery(hiveSql);

      // 7. 遍历结果集并批量插入MySQL
      int batchCount = 0;
      final int BATCH_SIZE = 100; // 批量提交大小，适配多年份Top20数据
      while (rs.next()) {
        int year = rs.getInt("year");
        String fips = rs.getString("FIPS");
        // 限制数值范围，避免超出MySQL DECIMAL(10,2)上限
        double humanImpactScore = Math.min(rs.getDouble("human_impact_score"), 99999999.99);
        int impactRank = rs.getInt("impact_rank");

        // 设置插入参数
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setString(2, fips);
        mysqlPstmt.setDouble(3, humanImpactScore);
        mysqlPstmt.setInt(4, impactRank);

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小提交
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          mysqlConn.commit();
          System.out.printf("已批量插入 %d 条人类活动影响排名数据%n", batchCount);
        }
      }

      // 处理剩余数据
      if (batchCount > 0 && batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        mysqlConn.commit();
        System.out.printf("最终提交剩余数据，总计插入 %d 条数据%n", batchCount);
      }

      System.out.println("人类活动影响排名数据已成功导入MySQL（american_data_analysis数据库）！");

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
      // 8. 关闭所有资源
      closeResources(rs, hiveStmt, hiveConn);
      try {
        if (mysqlPstmt != null) mysqlPstmt.close();
        if (mysqlConn != null) mysqlConn.close();
      } catch (SQLException e) {
        System.err.println("MySQL资源关闭异常：" + e.getMessage());
      }
      System.out.println("所有数据库连接已关闭");
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
      System.err.println("Hive资源关闭异常：" + e.getMessage());
    }
  }
}