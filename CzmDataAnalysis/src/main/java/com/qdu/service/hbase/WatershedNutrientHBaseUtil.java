package com.qdu.service.hbase;

import com.qdu.connection.*;
import com.qdu.connection.tool.HiveMaxIdQueryUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * 完整的流域数据处理管道
 * 1. 查询Hive最大ID
 * 2. 用这个ID从Hive导入数据到HBase
 * 3. 用这个ID从HBase查询前100大导入MySQL
 */
public class WatershedNutrientHBaseUtil {

  // HBase配置
  private static final String HBASE_NAMESPACE = "watershed_hbase";
  private static final String HBASE_TABLE = "nutrient_surplus";
  private static final String CF_BASIC = "cf_basic";
  private static final String CF_SURPLUS = "cf_surplus";

  // 存储当前批次ID（全局使用）
  private static int currentBatchId = 0;

  public static void main(String[] args) {
    System.out.println("=== 开始完整的流域数据处理 ===");

    try {
      // 步骤1: 获取当前最大ID
      currentBatchId = HiveMaxIdQueryUtil.getMaxId();
      System.out.println("当前批次ID: " + currentBatchId);

      if (currentBatchId <= 0) {
        System.out.println("无数据可处理");
        return;
      }

      // 步骤2: Hive → HBase（使用当前ID）
      importHiveToHBase();

      // 步骤3: HBase → MySQL（查询前100大）
      importTop100FromHBaseToMySQL();

      System.out.println("=== 处理完成 ===");

    } catch (Exception e) {
      System.err.println("处理失败: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 步骤2: 从Hive导入数据到HBase（使用currentBatchId）
   */
  private static void importHiveToHBase() throws Exception {
    System.out.println("\n步骤2: 从Hive导入数据到HBase...");

    // 从Hive查询数据（使用当前批次ID）
    List<HiveData> dataList = getDataFromHive();
    System.out.println("从Hive查询到 " + dataList.size() + " 条数据");

    if (dataList.isEmpty()) {
      System.out.println("Hive中没有该批次数据");
      return;
    }

    // 导入到HBase
    Connection hbaseConn = HBaseConnUtil.getHBaseConnection();
    Table table = hbaseConn.getTable(TableName.valueOf(HBASE_NAMESPACE+"."+HBASE_TABLE));

    try {
      List<Put> puts = new ArrayList<>();
      int count = 0;

      for (HiveData data : dataList) {
        // 构建rowkey: fips_year_id
        String rowkey = data.fips + "_" + data.year + "_" + currentBatchId;
        Put put = new Put(Bytes.toBytes(rowkey));

        // 基础信息列族（ID直接用currentBatchId）
        put.addColumn(Bytes.toBytes(CF_BASIC), Bytes.toBytes("id"),
                Bytes.toBytes(String.valueOf(currentBatchId)));
        put.addColumn(Bytes.toBytes(CF_BASIC), Bytes.toBytes("fips"),
                Bytes.toBytes(String.valueOf(data.fips)));
        put.addColumn(Bytes.toBytes(CF_BASIC), Bytes.toBytes("year"),
                Bytes.toBytes(String.valueOf(data.year)));

        // 盈余数据列族
        put.addColumn(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("n_ag_surplus_kgsqkm"),
                Bytes.toBytes(String.valueOf(data.nAgSurplus)));
        put.addColumn(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("p_ag_surplus_kgsqkm"),
                Bytes.toBytes(String.valueOf(data.pAgSurplus)));

        puts.add(put);
        count++;

        // 批量提交
        if (puts.size() >= 100) {
          table.put(puts);
          puts.clear();
          if (count % 1000 == 0) {
            System.out.println("已导入 " + count + " 条到HBase");
          }
        }
      }

      // 提交剩余数据
      if (!puts.isEmpty()) {
        table.put(puts);
      }

      System.out.println("总共导入 " + count + " 条数据到HBase（批次ID: " + currentBatchId + "）");

    } finally {
      table.close();
    }
  }

  /**
   * 从Hive查询数据（只查询当前批次ID的数据）
   */
  private static List<HiveData> getDataFromHive() throws Exception {
    List<HiveData> dataList = new ArrayList<>();

    java.sql.Connection hiveConn = HiveConnUtil.getHiveConnection();
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try {
      // 只查询当前批次ID的数据
      String sql = "SELECT FIPS, year, n_ag_surplus_kgsqkm, p_ag_surplus_kgsqkm " +
              "FROM watershed_nutrient_balance " +
              "WHERE id = ?";  // 使用当前批次ID

      pstmt = hiveConn.prepareStatement(sql);
      pstmt.setInt(1, currentBatchId);
      rs = pstmt.executeQuery();

      while (rs.next()) {
        HiveData data = new HiveData();
        data.fips = rs.getInt("FIPS");
        data.year = rs.getInt("year");
        data.nAgSurplus = rs.getDouble("n_ag_surplus_kgsqkm");
        data.pAgSurplus = rs.getDouble("p_ag_surplus_kgsqkm");
        dataList.add(data);
      }

    } finally {
      if (rs != null) rs.close();
      if (pstmt != null) pstmt.close();
      if (hiveConn != null) hiveConn.close();
    }

    return dataList;
  }

  /**
   * 步骤3: 从HBase查询前100大氮盈余数据并导入MySQL
   */
  private static void importTop100FromHBaseToMySQL() throws Exception {
    System.out.println("\n步骤3: 从HBase查询前100大并导入MySQL...");

    Connection hbaseConn = HBaseConnUtil.getHBaseConnection();
    Table table = hbaseConn.getTable(TableName.valueOf(HBASE_NAMESPACE+"."+HBASE_TABLE));

    List<HBaseData> allData = new ArrayList<>();

    try {
      // 扫描HBase中当前批次的数据
      Scan scan = new Scan();
      scan.addColumn(Bytes.toBytes(CF_BASIC), Bytes.toBytes("fips"));
      scan.addColumn(Bytes.toBytes(CF_BASIC), Bytes.toBytes("year"));
      scan.addColumn(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("n_ag_surplus_kgsqkm"));
      scan.addColumn(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("p_ag_surplus_kgsqkm"));

      ResultScanner scanner = table.getScanner(scan);

      for (Result result : scanner) {
        // 检查是否属于当前批次（通过rowkey判断）
        String rowkey = Bytes.toString(result.getRow());
        if (rowkey.endsWith("_" + currentBatchId)) {
          HBaseData data = new HBaseData();

          byte[] fipsBytes = result.getValue(Bytes.toBytes(CF_BASIC), Bytes.toBytes("fips"));
          byte[] yearBytes = result.getValue(Bytes.toBytes(CF_BASIC), Bytes.toBytes("year"));
          byte[] nBytes = result.getValue(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("n_ag_surplus_kgsqkm"));
          byte[] pBytes = result.getValue(Bytes.toBytes(CF_SURPLUS), Bytes.toBytes("p_ag_surplus_kgsqkm"));

          if (fipsBytes != null && yearBytes != null && nBytes != null) {
            data.fips = Integer.parseInt(Bytes.toString(fipsBytes));
            data.year = Integer.parseInt(Bytes.toString(yearBytes));
            data.nSurplus = Double.parseDouble(Bytes.toString(nBytes));
            data.pSurplus = pBytes != null ? Double.parseDouble(Bytes.toString(pBytes)) : 0.0;

            allData.add(data);
          }
        }
      }
      scanner.close();

      System.out.println("从HBase扫描到 " + allData.size() + " 条数据");

      if (allData.isEmpty()) {
        System.out.println("HBase中没有该批次数据");
        return;
      }

      // 按氮盈余降序排序，取前100大
      allData.sort((a, b) -> Double.compare(b.nSurplus, a.nSurplus));

      int limit = Math.min(100, allData.size());
      List<HBaseData> top100 = allData.subList(0, limit);

      System.out.println("氮盈余最大值: " + top100.get(0).nSurplus);
      System.out.println("氮盈余最小值（前100）: " + top100.get(limit-1).nSurplus);

      // 导入到MySQL
      importToMySQL(top100);

    } finally {
      table.close();
    }
  }

  /**
   * 导入数据到MySQL
   */
  private static void importToMySQL(List<HBaseData> dataList) throws Exception {
    java.sql.Connection mysqlConn = MysqlConnUtil.getMysqlConnection();
    PreparedStatement pstmt = null;

    try {
      // 删除该批次的旧数据（确保不重复）
      pstmt = mysqlConn.prepareStatement("DELETE FROM watershed_surplus_trend WHERE hid = ?");
      pstmt.setInt(1, currentBatchId);
      pstmt.executeUpdate();
      pstmt.close();

      // 插入新数据（MySQL的id是自增的，不需要设置）
      pstmt = mysqlConn.prepareStatement(
              "INSERT INTO watershed_surplus_trend (fips, year, n_surplus, p_surplus, hid) VALUES (?, ?, ?, ?, ?)"
      );

      int count = 0;
      for (HBaseData data : dataList) {
        pstmt.setString(1, String.valueOf(data.fips));
        pstmt.setInt(2, data.year);
        pstmt.setDouble(3, data.nSurplus);
        pstmt.setDouble(4, data.pSurplus);
        pstmt.setInt(5, currentBatchId);  // hid就是当前批次ID
        pstmt.addBatch();
        count++;

        if (count % 50 == 0) {
          pstmt.executeBatch();
        }
      }

      pstmt.executeBatch();
      MysqlConnUtil.commit(mysqlConn);

      System.out.println("成功导入 " + count + " 条数据到MySQL（批次ID: " + currentBatchId + "）");

    } catch (Exception e) {
      MysqlConnUtil.rollback(mysqlConn);
      throw e;
    } finally {
      MysqlConnUtil.closeConnection(mysqlConn, pstmt);
    }
  }

  /**
   * 数据类：Hive数据
   */
  static class HiveData {
    int fips;
    int year;
    double nAgSurplus;
    double pAgSurplus;
  }

  /**
   * 数据类：HBase数据
   */
  static class HBaseData {
    int fips;
    int year;
    double nSurplus;
    double pSurplus;
  }
}