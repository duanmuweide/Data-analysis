package com.qdu.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * MySQL JDBC连接工具类
 */
public class MysqlConnUtil {
  // MySQL连接配置（需替换为实际环境）
  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/american_data_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
  private static final String MYSQL_USER = "root";
  private static final String MYSQL_PASSWORD = "Czm982376";

  static {
    // 静态代码块加载MySQL驱动
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      System.err.println("MySQL驱动加载失败：" + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 获取MySQL连接（默认关闭自动提交）
   * @return Connection
   * @throws SQLException 连接异常
   */
  public static Connection getMysqlConnection() throws SQLException {
    Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
    conn.setAutoCommit(false); // 关闭自动提交，便于批量操作
    return conn;
  }

  /**
   * 关闭MySQL连接
   * @param conn 连接对象
   */
  public static void closeConnection(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        System.err.println("关闭MySQL连接失败：" + e.getMessage());
      }
    }
  }

  /**
   * 关闭MySQL连接及PreparedStatement
   * @param conn 连接对象
   * @param pstmt PreparedStatement对象
   */
  public static void closeConnection(Connection conn, java.sql.PreparedStatement pstmt) {
    if (pstmt != null) {
      try {
        pstmt.close();
      } catch (SQLException e) {
        System.err.println("关闭MySQL PreparedStatement失败：" + e.getMessage());
      }
    }
    closeConnection(conn);
  }

  /**
   * 提交事务
   * @param conn 连接对象
   * @throws SQLException 提交异常
   */
  public static void commit(Connection conn) throws SQLException {
    if (conn != null && !conn.getAutoCommit()) {
      conn.commit();
    }
  }

  /**
   * 回滚事务
   * @param conn 连接对象
   */
  public static void rollback(Connection conn) throws SQLException {
    if (conn != null && !conn.getAutoCommit()) {
      try {
        conn.rollback();
        System.out.println("MySQL事务已回滚");
      } catch (SQLException e) {
        System.err.println("MySQL事务回滚失败：" + e.getMessage());
      }
    }
  }
}