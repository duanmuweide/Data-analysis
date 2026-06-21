package com.qdu.hbase;

import com.qdu.connection.HBaseConnUtil;
import com.qdu.connection.HiveConnUtil;
import com.qdu.connection.MysqlConnUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class CrimeHBaseImportUtil {

    private static final String HBASE_NAMESPACE = "default";
    private static final String HBASE_TABLE = "crime_incidents";
    private static final String COLUMN_FAMILY = "cf";

    // 可配置目标州（或从参数读取）
    private static final String TARGET_STATE = "MD";

    public static void main(String[] args) {
        System.out.println("=== 开始犯罪数据处理管道 ===");
        try {
            // 步骤1: Hive → HBase
            importHiveToHBase();

            // 步骤2: HBase → MySQL（按州）
            importCrimeFromHBaseToMySQL();

            System.out.println("=== 处理完成 ===");
        } catch (Exception e) {
            System.err.println("处理失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /** 步骤1: 从 Hive 导入到 HBase */
    private static void importHiveToHBase() throws Exception {
        System.out.println("\n步骤1: 从 Hive 导入数据到 HBase...");

        List<HiveCrimeData> dataList = fetchFromHive();
        System.out.println("从 Hive 查询到 " + dataList.size() + " 条记录");

        if (dataList.isEmpty()) return;

        Connection hbaseConn = HBaseConnUtil.getHBaseConnection();
        Table table = hbaseConn.getTable(TableName.valueOf(HBASE_NAMESPACE, HBASE_TABLE));

        try {
            List<Put> puts = new ArrayList<>();
            int count = 0;

            for (HiveCrimeData data : dataList) {
                // 安全检查：确保必要字段非空
                if (data.incidentId == null || data.state == null || data.timestamp == null) {
                    continue; // 跳过无效数据
                }

                String cleanCity = data.city != null ? data.city.replaceAll("\\s+", "") : "UNKNOWN";
                String cleanTs = data.timestamp.replaceAll("[-:\\s]", "");

                int salt = Math.abs(data.hashCode()) % 10;
                String rowkey = salt + "_" + data.state + cleanCity + data.incidentId + cleanTs;

                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("incident_id"), Bytes.toBytes(data.incidentId));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("state"), Bytes.toBytes(data.state));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("city"), Bytes.toBytes(data.city));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("victims"), Bytes.toBytes(data.victims));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("timestamp"), Bytes.toBytes(data.timestamp));

                puts.add(put);
                count++;

                if (puts.size() >= 100) {
                    table.put(puts);
                    puts.clear();
                    if (count % 1000 == 0) {
                        System.out.println("已导入 " + count + " 条到 HBase");
                    }
                }
            }

            if (!puts.isEmpty()) {
                table.put(puts);
            }
            System.out.println("总共导入 " + count + " 条数据到 HBase");
        } finally {
            table.close();
            hbaseConn.close();
        }
    }
    private static List<HiveCrimeData> fetchFromHive() throws Exception {
        List<HiveCrimeData> list = new ArrayList<>();
        java.sql.Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = HiveConnUtil.getHiveConnection();
            // ✅ 修正：使用 dispatch_time 并别名为 timestamp
            String sql = "SELECT incident_id, state, city, victims, dispatch_time AS `timestamp` FROM crime_incidents_external";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                HiveCrimeData data = new HiveCrimeData();
                data.incidentId = rs.getString("incident_id");
                data.state = rs.getString("state");
                data.city = rs.getString("city");
                data.victims = rs.getString("victims");
                data.timestamp = rs.getString("timestamp"); // 来自 dispatch_time
                list.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
        return list;
    }

    /** 步骤2: 从 HBase 扫描指定州，导入 MySQL */
    private static void importCrimeFromHBaseToMySQL() throws Exception {
        System.out.println("\n步骤2: 从 HBase 扫描州 '" + TARGET_STATE + "' 并导入 MySQL...");

        Connection hbaseConn = HBaseConnUtil.getHBaseConnection();
        Table table = hbaseConn.getTable(TableName.valueOf(HBASE_NAMESPACE, HBASE_TABLE));

        List<CrimeRecord> records = new ArrayList<>();
        Scan scan = new Scan();
        String startRow = "v1_" + TARGET_STATE + "_";
        String stopRow = "v1_" + TARGET_STATE + "~"; // ~ 的 ASCII > _
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("incident_id"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("city"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("victims"));
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("timestamp"));

        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result : scanner) {
                CrimeRecord rec = new CrimeRecord();
                rec.rowkey = Bytes.toString(result.getRow());
                rec.incidentId = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("incident_id")));
                rec.city = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("city")));
                rec.victims = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("victims")));
                rec.timestamp = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("timestamp")));
                records.add(rec);
            }
        } finally {
            scanner.close();
            table.close();
            hbaseConn.close();
        }

        System.out.println("扫描到 " + records.size() + " 条 " + TARGET_STATE + " 州的记录");

        if (!records.isEmpty()) {
            writeToMySQL(records);
        }
    }
    private static void writeToMySQL(List<CrimeRecord> records) throws Exception {
        java.sql.Connection mysqlConn = null;
        PreparedStatement pstmt = null;
        try {
            mysqlConn = MysqlConnUtil.getMysqlConnection();
            mysqlConn.setAutoCommit(false);

            // 清空旧数据（可选）
            pstmt = mysqlConn.prepareStatement("DELETE FROM crime_trend_analysis WHERE state = ?");
            pstmt.setString(1, TARGET_STATE);
            pstmt.executeUpdate();
            pstmt.close();

            pstmt = mysqlConn.prepareStatement(
                    "INSERT INTO crime_trend_analysis (rowkey, incident_id, state, city, victims, timestamp) VALUES (?, ?, ?, ?, ?, ?)"
            );

            int count = 0;
            for (CrimeRecord r : records) {
                pstmt.setString(1, r.rowkey);
                pstmt.setString(2, r.incidentId);
                pstmt.setString(3, TARGET_STATE); // 明确写入州
                pstmt.setString(4, r.city);
                pstmt.setString(5, r.victims);

                // === 关键修改：安全处理 timestamp 字段 ===
                String tsStr = r.timestamp;
                if (tsStr == null || tsStr.trim().isEmpty()) {
                    pstmt.setNull(6, java.sql.Types.TIMESTAMP);
                } else {
                    try {
                        // 假设原始格式为 "yyyy-MM-dd HH:mm:ss"（来自 Hive 的 dispatch_time）
                        // 如果格式不同，请调整解析逻辑
                        java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(tsStr, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        java.sql.Timestamp sqlTs = java.sql.Timestamp.valueOf(ldt);
                        pstmt.setTimestamp(6, sqlTs);
                    } catch (Exception e) {
                        // 解析失败也设为 NULL，并可选打印警告
                        System.err.println("警告: 无法解析时间戳 '" + tsStr + "'，设为 NULL");
                        pstmt.setNull(6, java.sql.Types.TIMESTAMP);
                    }
                }
                // ======================================

                pstmt.addBatch();
                count++;
                if (count % 50 == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                }
            }
            if (count % 50 != 0) {
                pstmt.executeBatch();
            }
            mysqlConn.commit();
            System.out.println("成功导入 " + count + " 条到 MySQL");
        } catch (Exception e) {
            if (mysqlConn != null) {
                mysqlConn.rollback();
            }
            throw e;
        } finally {
            MysqlConnUtil.closeConnection(mysqlConn, pstmt);
        }
    }

    // --- 数据类 ---
    static class HiveCrimeData {
        String incidentId;
        String state;
        String city;
        String victims;
        String timestamp;
    }

    static class CrimeRecord {
        String rowkey;
        String incidentId;
        String city;
        String victims;
        String timestamp;
    }
}