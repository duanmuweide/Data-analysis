package com.qdu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive自定义UDF：装修分类
 * 逻辑：精装/简装/毛坯→对应分类；其他→其它
 */
public class DecorationClassifyUDF extends UDF {

    public Text evaluate(Text decoration) {
        // 处理空值
        if (decoration == null || decoration.toString().trim().isEmpty()) {
            return new Text("其它");
        }

        String decoStr = decoration.toString().trim();
        // 装修分类逻辑
        switch (decoStr) {
            case "精装":
                return new Text("精装");
            case "简装":
                return new Text("简装");
            case "毛坯":
                return new Text("毛坯");
            default:
                // 豪装、简装自住、毛坯未装等归为其它
                return new Text("其它");
        }
    }
}
