package com.qdu.service;

import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;

import java.sql.*;

/**
 * 流域农业氮盈余与大气氮沉降相关性分析（含自定义UDF + MySQL导入）
 * 复用统一连接工具类，仅处理最新批次（maxId）数据
 */
public class NitrogenSurplusAtmDepCorr {
  // 仅保留业务配置，连接配置移到工具类
  private static final String MYSQL_TARGET_TABLE = "nutrient_surplus_atm_dep_corr"; // MySQL目标表名
  private static final int BATCH_SIZE = 100; // 批量提交大小

  // UDF相关SQL（保持不变）
  private static final String ADD_JAR_SQL = "add jar /home/master/CzmDataAnalysis-1.0-SNAPSHOT.jar";
  private static final String CREATE_UDF_SQL = "CREATE TEMPORARY FUNCTION calc_corr_aux AS 'com.qdu.CorrCoefficientUDF'";

  public static void main(String[] args) throws SQLException {
    // 声明数据库资源
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

      // 3. 加载UDF JAR（保持不变）
      try {
        hiveStmt.execute(ADD_JAR_SQL);
        System.out.println("UDF JAR加载成功！");
      } catch (SQLException e) {
        System.err.println("UDF JAR加载失败：" + e.getMessage());
        throw e;
      }

      // 4. 创建临时UDF（保持不变）
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

      // 5. 构建核心查询SQL（新增id字段 + 动态maxId）
      String QUERY_SQL =
              "SELECT " +
                      "    id, " +  // 新增批次ID
                      "    year, " +
                      "    ROUND(AVG(n_ag_surplus_kgsqkm), 3) AS avg_n_ag_surplus, " +
                      "    ROUND(AVG(n_atm_dep_kgsqkm), 3) AS avg_n_atm_dep, " +
                      "    calc_corr_aux(COLLECT_LIST(n_ag_surplus_kgsqkm), COLLECT_LIST(n_atm_dep_kgsqkm)) AS corr_coefficient " +
                      "FROM watershed_nutrient_balance " +
                      "WHERE id = " + maxId + " " +  // 动态传入最大批次ID
                      "  AND pmod(hash(FIPS), 8) BETWEEN 1 AND 4 " +
                      "  AND n_ag_surplus_kgsqkm IS NOT NULL " +
                      "  AND n_atm_dep_kgsqkm IS NOT NULL " +
                      "GROUP BY id, year " +  // GROUP BY新增id
                      "ORDER BY year ASC";

      // 6. 预编译MySQL插入语句（新增hid列，放在最后）
      String mysqlInsertSql = String.format(
              "INSERT INTO %s (year, avg_n_ag_surplus, avg_n_atm_dep, corr_coefficient, hid) " +  // 新增hid列
                      "VALUES (?, ?, ?, ?, ?)", MYSQL_TARGET_TABLE);
      mysqlPstmt = mysqlConn.prepareStatement(mysqlInsertSql);

      // 7. 执行Hive查询
      System.out.println("开始执行Hive查询...");
      rs = hiveStmt.executeQuery(QUERY_SQL);

      // 8. 遍历结果集，批量插入MySQL（新增hid参数）
      int batchCount = 0;
      System.out.println("开始向MySQL导入数据...");
      while (rs.next()) {
        // 获取查询结果（新增id字段）
        int hid = rs.getInt("id");  // 批次ID -> MySQL的hid列
        int year = rs.getInt("year");
        double avgSurplus = rs.getDouble("avg_n_ag_surplus");
        double avgAtmDep = rs.getDouble("avg_n_atm_dep");
        // 兼容相关系数可能为字符串/数值类型，统一用getString后转Double
        double corrCoeff = Double.parseDouble(rs.getString("corr_coefficient"));

        // 设置MySQL参数（最后一个参数为hid）
        mysqlPstmt.setInt(1, year);
        mysqlPstmt.setDouble(2, avgSurplus);
        mysqlPstmt.setDouble(3, avgAtmDep);
        mysqlPstmt.setDouble(4, corrCoeff);
        mysqlPstmt.setInt(5, hid);  // 新增：设置hid参数

        // 添加到批处理
        mysqlPstmt.addBatch();
        batchCount++;

        // 达到批量大小，提交一次
        if (batchCount % BATCH_SIZE == 0) {
          mysqlPstmt.executeBatch();
          MysqlConnUtil.commit(mysqlConn);  // 复用工具类提交方法
          System.out.printf("已批量导入 %d 条数据（批次ID：%d）%n", batchCount, maxId);
        }
      }

      // 处理剩余未提交的数据
      if (batchCount > 0 && batchCount % BATCH_SIZE != 0) {
        mysqlPstmt.executeBatch();
        MysqlConnUtil.commit(mysqlConn);
        System.out.printf("最终提交剩余数据，总计导入 %d 条数据（批次ID：%d）%n", batchCount, maxId);
      }

      System.out.println("\n===== 数据导入完成！ =====");
      System.out.println("导入表名：" + MYSQL_TARGET_TABLE);
      System.out.println("数据来源：流域农业氮盈余与大气氮沉降相关性分析结果");
      System.out.println("本次批次ID：" + maxId);

    } catch (NumberFormatException e) {
      System.err.println("相关系数转换为数值失败：" + e.getMessage());
      e.printStackTrace();
    } catch (SQLException e) {
      System.err.println("SQL执行异常：" + e.getMessage());
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
      System.out.println("所有数据库资源已关闭");
    }
  }
}