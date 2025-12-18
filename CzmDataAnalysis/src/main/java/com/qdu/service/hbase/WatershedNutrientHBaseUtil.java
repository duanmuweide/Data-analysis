package com.qdu.service.hbase;

import com.qdu.connection.HBaseConnUtil;
import com.qdu.connection.MysqlConnUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 复用统一连接工具类 → HBase查询指定FIPS流域数据 → 批量写入MySQL
 */
public class WatershedNutrientHBaseUtil {
  // -------------------------- 1. 仅保留业务核心配置 --------------------------
  // HBase表名
  private static final String HBASE_TABLE = "watershed_hbase.nutrient_surplus";
  // HBase列定义
  private static final byte[] CF_BASIC = Bytes.toBytes("cf_basic");
  private static final byte[] CF_SURPLUS = Bytes.toBytes("cf_surplus");
  private static final byte[] COL_YEAR = Bytes.toBytes("year");
  private static final byte[] COL_N_SURPLUS = Bytes.toBytes("n_ag_surplus_kgsqkm");
  private static final byte[] COL_P_SURPLUS = Bytes.toBytes("p_ag_surplus_kgsqkm");
  // MySQL目标表名
  private static final String MYSQL_TABLE = "watershed_surplus_trend";
  // 批量提交大小
  private static final int BATCH_SIZE = 100;

  // -------------------------- 2. 数据实体类（极简） --------------------------
  static class TrendData {
    String fips;
    int year;
    double nSurplus;
    double pSurplus;

    TrendData(String fips, int year, double nSurplus, double pSurplus) {
      this.fips = fips;
      this.year = year;
      this.nSurplus = nSurplus;
      this.pSurplus = pSurplus;
    }
  }

  // -------------------------- 3. HBase查询（复用HBaseConnUtil） --------------------------
  private static List<TrendData> queryHBase(String fips, int startYear, int endYear) throws IOException {
    List<TrendData> trendList = new ArrayList<>();
    Table table = null;
    ResultScanner scanner = null;

    try {
      // 复用HBase连接工具类获取连接
      org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseConnUtil.getHBaseConnection();
      table = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE));

      // 构建RowKey范围（反转FIPS+年份）
      String revFips = new StringBuilder(fips).reverse().toString();
      String startRow = revFips + "_" + String.format("%04d", startYear);
      String stopRow = revFips + "_" + String.format("%04d", endYear + 1);

      // 扫描HBase数据
      Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
      scan.addColumn(CF_BASIC, COL_YEAR);
      scan.addColumn(CF_SURPLUS, COL_N_SURPLUS);
      scan.addColumn(CF_SURPLUS, COL_P_SURPLUS);

      scanner = table.getScanner(scan);
      for (Result res : scanner) {
        int year = Bytes.toInt(res.getValue(CF_BASIC, COL_YEAR));
        double n = res.containsColumn(CF_SURPLUS, COL_N_SURPLUS) ?
                Bytes.toDouble(res.getValue(CF_SURPLUS, COL_N_SURPLUS)) : 0.0;
        double p = res.containsColumn(CF_SURPLUS, COL_P_SURPLUS) ?
                Bytes.toDouble(res.getValue(CF_SURPLUS, COL_P_SURPLUS)) : 0.0;
        trendList.add(new TrendData(fips, year, n, p));
      }
      System.out.println("✅ HBase查询完成，共" + trendList.size() + "条数据（FIPS：" + fips + "）");

    } finally {
      // 关闭HBase资源
      if (scanner != null) scanner.close();
      if (table != null) table.close();
    }
    return trendList;
  }

  // -------------------------- 4. 写入MySQL（复用MysqlConnUtil） --------------------------
  private static void writeToMySQL(List<TrendData> trendList) throws SQLException {
    if (trendList.isEmpty()) {
      System.out.println("⚠️ 无数据可写入MySQL");
      return;
    }

    PreparedStatement pstmt = null;
    try {
      // 复用MySQL连接工具类获取连接
      Connection mysqlConn = MysqlConnUtil.getMysqlConnection();
      // 批量插入/更新SQL
      String sql = "INSERT INTO " + MYSQL_TABLE + " (fips, year, n_surplus, p_surplus) VALUES (?, ?, ?, ?) " +
              "ON DUPLICATE KEY UPDATE n_surplus=VALUES(n_surplus), p_surplus=VALUES(p_surplus)";
      pstmt = mysqlConn.prepareStatement(sql);

      int count = 0;
      for (TrendData data : trendList) {
        pstmt.setString(1, data.fips);
        pstmt.setInt(2, data.year);
        pstmt.setDouble(3, data.nSurplus);
        pstmt.setDouble(4, data.pSurplus);
        pstmt.addBatch();
        count++;

        // 批量提交
        if (count % BATCH_SIZE == 0) {
          pstmt.executeBatch();
          MysqlConnUtil.commit(mysqlConn);
          System.out.println("✅ MySQL已写入" + count + "条数据");
        }
      }

      // 提交剩余数据
      pstmt.executeBatch();
      MysqlConnUtil.commit(mysqlConn);
      System.out.println("✅ MySQL写入完成，累计" + count + "条数据");

    } catch (SQLException e) {
      // 复用工具类回滚事务
      MysqlConnUtil.rollback(MysqlConnUtil.getMysqlConnection());
      System.err.println("❌ MySQL写入失败：" + e.getMessage());
      throw e;
    } finally {
      // 关闭MySQL资源
      if (pstmt != null) pstmt.close();
      MysqlConnUtil.closeConnection(MysqlConnUtil.getMysqlConnection());
    }
  }

  // -------------------------- 5. 主方法（极简入口，保留FIPS手动传入） --------------------------
  public static void main(String[] args) {
    try {
      // 手动指定查询参数（核心：保留FIPS手动传入，无maxId逻辑）
      String queryFips = "10005"; // 可直接修改此处FIPS值
      int startYear = 2010;
      int endYear = 2020;

      // 全流程执行
      List<TrendData> trendData = queryHBase(queryFips, startYear, endYear);
      writeToMySQL(trendData);

      // 关闭HBase连接（复用工具类方法）
      HBaseConnUtil.closeConnection();
      System.out.println("\n🎉 全流程执行完成：HBase(" + queryFips + ") → MySQL");

    } catch (Exception e) {
      System.err.println("\n❌ 执行失败：" + e.getMessage());
      e.printStackTrace();
    }
  }
}
