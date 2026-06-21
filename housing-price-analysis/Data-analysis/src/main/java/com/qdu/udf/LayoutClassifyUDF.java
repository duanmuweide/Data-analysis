package com.qdu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive自定义UDF：户型分类
 * 逻辑：一室/二室→小户型；三室/四室→中户型；其他→大户型
 */
public class LayoutClassifyUDF extends UDF {

    /**
     * 核心方法：Hive调用的执行方法
     * @param layout 原始户型字符串（如"一室"、"三室两厅"、"别墅"）
     * @return 分类结果：小户型/中户型/大户型
     */
    public Text evaluate(Text layout) {
        // 处理空值
        if (layout == null || layout.toString().trim().isEmpty()) {
            return new Text("大户型");
        }

        String layoutStr = layout.toString().trim();
        // 户型分类逻辑
        if (layoutStr.contains("1室") || layoutStr.contains("2室")) {
            return new Text("小户型");
        } else if (layoutStr.contains("3室") || layoutStr.contains("4室")) {
            return new Text("中户型");
        } else {
            // 五室及以上、别墅、复式等归为大户型
            return new Text("大户型");
        }
    }
}