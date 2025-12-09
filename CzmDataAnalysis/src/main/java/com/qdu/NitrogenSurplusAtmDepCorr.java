package com.qdu;

import java.sql.*;
import java.util.Properties;

/**
 * 流域农业氮盈余与大气氮沉降相关性分析（含自定义UDF + MySQL导入）
 */
public class NitrogenSurplusAtmDepCorr {
  // Hive连接参数（保持不变）
  private static final String HIVE_URL = "jdbc:hive2://master-pc:10000/data_analysis";
  private static final String HIVE_USER = "master";
  private static final String HIVE_PASSWORD = "";

  // MySQL连接配置（适配你的项目环境，与之前模板一致）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376";
  private static final String MYSQL_TARGET_TABLE = "nutrient_surplus_atm_dep_corr"; // MySQL目标表名

  // UDF相关SQL（保持不变）
  private static final String ADD_JAR_SQL = "add jar /home/master/CzmDataAnalysis-1.0-SNAPSHOT.jar";
  private static final String CREATE_UDF_SQL = "CREATE TEMPORARY FUNCTION calc_corr_aux AS 'com.qdu.CorrCoefficientUDF'";

  // 核心查询SQL（保持不变）
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
    // Hive相关资源（保持不变）
    Connection hiveConn = null;
    Statement hiveStmt = null;
    ResultSet rs = null;

    // MySQL相关资源（新增）
    Connection mysqlConn = null;
    PreparedStatement mysqlPstmt = null;

    try {
      // 1. 加载驱动 + 创建Hive连接（保持不变）
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Class.forName("com.mysql.cj.jdbc.Driver"); // 新增MySQL驱动加载
      Properties hiveProps = new Properties();
      hiveProps.setProperty("user", HIVE_USER);
      hiveProps.setProperty("password", HIVE_PASSWORD);
      hiveConn = DriverManager.getConnection(HIVE_URL, hiveProps);
      hiveStmt = hiveConn.createStatement();

      // 2. 加载UDF JAR（保持不变）
      try {
        hiveStmt.execute(ADD_JAR_SQL);
        System.out.println("UDF JAR加载成功！");
      } catch (SQLException e) {
        System.err.println("UDF JAR加载失败：" + e.getMessage());
        throw e;
      }

      // 3. 创建临时UDF（保持不变）
      try {
        hiveStmt.execute(CREATE_UDF_SQL);
        System.out.println("自定义函数calc_corr_aux创建成功！");
      } catch (SQLException e) {
        if (e.getMessage().contains("Function 'calc_corr_aux' already exists")) {
          System.out.println("自定义函数已存在，跳过创建！");
        } else {
          throw e;
        }
      }

      // 4. 建立MySQL连接（新增）
      mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
      mysqlConn.setAutoCommit(false); // 关闭自动提交，开启批量插入
      System.out.println("MySQL连接成功！");

      // 5. 预编译MySQL插入语句（新增）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, avg_n_ag_surplus, avg_n_atm_dep, corr_coefficient) " +
                      "VALUES (?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 6. 执行Hive查询（保持不变）
      System.out.println("开始执行Hive查询...");
      rs = hiveStmt.executeQuery(QUERY_SQL);

      // 7. 遍历结果集，批量插入MySQL（替换原终端打印逻辑）
      int batchCount = 0;
      final int BATCH_SIZE = 100; // 批量提交大小，适配数据量
      System.out.println("开始向MySQL导入数据...");
      while (rs.next()) {
        // 获取查询结果（字段顺序与SQL一致）
        int year = rs.getInt("year");
        double avgSurplus = rs.getDouble("avg_n_ag_surplus");
        double avgAtmDep = rs.getDouble("avg_n_atm_dep");
        // 兼容相关系数可能为字符串/数值类型，统一用getString后转Double
        double corrCoeff = Double.parseDouble(rs.getString("corr_coefficient"));

        // 设置MySQL参数（顺序与插入语句字段对应）
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setDouble(2, avgSurplus);
        mysqlPstmt.setDouble(3, avgAtmDep);
        mysqlPstmt.setDouble(4, corrCoeff);

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小，提交一次
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          mysqlConn.commit();
          System.out.printf("已批量导入 %d 条数据%n", batchCount);
        }
      }

      // 处理剩余未提交的数据
      if (batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        mysqlConn.commit();
        System.out.printf("最终提交剩余数据，总计导入 %d 条数据%n", batchCount);
      }

      System.out.println("\n===== 数据导入完成！ =====");
      System.out.println("导入表名：" + MYSQL_TARGET_TABLE);
      System.out.println("数据来源：流域农业氮盈余与大气氮沉降相关性分析结果");

    } catch (ClassNotFoundException e) {
      System.err.println("驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    } catch (NumberFormatException e) {
      System.err.println("相关系数转换为数值失败：" + e.getMessage());
      e.printStackTrace();
    } catch (SQLException e) {
      System.err.println("SQL执行异常：" + e.getMessage());
      e.printStackTrace();
      // MySQL事务回滚（新增）
      try {
        if (mysqlConn != null && !mysqlConn.isClosed()) {
          mysqlConn.rollback();
          System.out.println("MySQL事务已回滚，避免数据不一致");
        }
      } catch (SQLException rollbackEx) {
        System.err.println("事务回滚失败：" + rollbackEx.getMessage());
      }
    } finally {
      // 关闭所有资源（新增MySQL资源关闭）
      closeResources(rs, hiveStmt, hiveConn);
      closeMysqlResources(mysqlPstmt, mysqlConn);
    }
  }

  /**
   * 关闭Hive相关资源（保持不变）
   */
  private static void closeResources(ResultSet rs, Statement stmt, Connection conn) {
    try {
      if (rs != null) rs.close();
      if (stmt != null) stmt.close();
      if (conn != null) conn.close();
      System.out.println("Hive资源已关闭");
    } catch (SQLException e) {
      System.err.println("Hive资源关闭异常：" + e.getMessage());
    }
  }

  /**
   * 关闭MySQL相关资源（新增）
   */
  private static void closeMysqlResources(PreparedStatement pstmt, Connection conn) {
    try {
      if (pstmt != null) pstmt.close();
      if (conn != null) conn.close();
      System.out.println("MySQL资源已关闭");
    } catch (SQLException e) {
      System.err.println("MySQL资源关闭异常：" + e.getMessage());
    }
  }
}