package com.qdu.hive;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RiskLevelUDF extends UDF {

    // 主要方法：接收 Hive 的 string 类型（即 Text）
    public Text evaluate(Text victims) {
        if (victims == null || victims.getLength() == 0) {
            return new Text("Unknown");
        }
        String str = victims.toString().trim();
        if (str.isEmpty()) {
            return new Text("Unknown");
        }

        try {
            int v = Integer.parseInt(str);
            if (v >= 3) return new Text("High");
            else if (v >= 2) return new Text("Medium");
            else if (v >= 0) return new Text("Low");
            else return new Text("Unknown"); // 负数视为无效
        } catch (NumberFormatException e) {
            return new Text("Unknown"); // 非数字字符串（如 "N/A"）
        }
    }

    // 兼容 Java String（某些上下文可能用到）
    public Text evaluate(String victims) {
        if (victims == null || victims.trim().isEmpty()) {
            return new Text("Unknown");
        }
        try {
            int v = Integer.parseInt(victims.trim());
            if (v >= 3) return new Text("High");
            else if (v >= 2) return new Text("Medium");
            else if (v >= 0) return new Text("Low");
            else return new Text("Unknown");
        } catch (NumberFormatException e) {
            return new Text("Unknown");
        }
    }
}