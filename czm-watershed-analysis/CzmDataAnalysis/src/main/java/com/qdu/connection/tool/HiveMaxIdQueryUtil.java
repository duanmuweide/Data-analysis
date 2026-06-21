package com.qdu.connection.tool;

import com.qdu.connection.HiveConnUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Hive表watershed_nutrient_balance最大ID查询工具类
 * 用于获取最新上传数据集的批次标识（id）
 */
public class HiveMaxIdQueryUtil {

  // Hive查询最大ID的SQL语句
  private static final String MAX_ID_HQL = "SELECT MAX(id) as max_id FROM watershed_nutrient_balance";

  /**
   * 查询Hive表中最大的id值
   * @return 最大id（无数据时返回0）
   * @throws SQLException 数据库操作异常
   */
  public static int getMaxId() throws SQLException {
    Connection hiveConn = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    int maxId = 0; // 默认无数据时返回0

    try {
      // 1. 获取Hive连接（复用已封装的连接工具类）
      hiveConn = HiveConnUtil.getHiveConnection();

      // 2. 预编译查询SQL
      pstmt = hiveConn.prepareStatement(MAX_ID_HQL);

      // 3. 执行查询
      rs = pstmt.executeQuery();

      // 4. 解析结果集
      if (rs.next()) {
        maxId = rs.getInt("max_id");
        // 处理NULL情况（表无数据时MAX(id)返回NULL）
        if (rs.wasNull()) {
          maxId = 0;
        }
      }

      System.out.printf("成功查询到Hive表watershed_nutrient_balance的最大ID：%d%n", maxId);

    } catch (SQLException e) {
      System.err.println("查询Hive最大ID失败：" + e.getMessage());
      throw e; // 抛出异常让上层处理
    } finally {
      // 5. 关闭资源（逆序关闭）
      try {
        if (rs != null) rs.close();
        if (pstmt != null) pstmt.close();
      } catch (SQLException e) {
        System.err.println("关闭查询资源失败：" + e.getMessage());
      }
      HiveConnUtil.closeConnection(hiveConn); // 复用连接关闭方法
    }

    return maxId;
  }

  // 测试方法（可选）
  public static void main(String[] args) {
    try {
      int maxId = getMaxId();
      System.out.println("测试查询最大ID结果：" + maxId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}