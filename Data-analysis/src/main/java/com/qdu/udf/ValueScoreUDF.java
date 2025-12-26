package com.qdu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

/**
 * 性价比评分 UDF
 * 输入：区域、面积段、户型分类、装修分类、电梯（1/0）
 * 输出：整数评分（INT），范围约 0~53
 */
public class ValueScoreUDF extends UDF {

    public IntWritable evaluate(
            String district,
            String areaRange,
            String layoutCategory,
            String decorationCategory,
            Integer elevatorInt) {

        // 空值安全检查
        if (district == null || areaRange == null ||
                layoutCategory == null || decorationCategory == null) {
            return null;
        }

        int score = 0;

        // 1. 区域得分
        switch (district) {
            case "海淀":   score += 16; break;
            case "朝阳":   score += 15; break;
            case "西城":   score += 14; break;
            case "东城":   score += 13; break;
            case "大兴":   score += 12; break;
            case "顺义":   score += 11; break;
            case "丰台":   score += 10; break;
            case "昌平":   score += 9;  break;
            case "通州":   score += 8;  break;
            case "石景山": score += 7;  break;
            case "房山":   score += 6;  break;
            case "怀柔":   score += 5;  break;
            case "平谷":   score += 4;  break;
            case "密云":   score += 3;  break;
            case "门头沟": score += 2;  break;
            case "延庆":   score += 1;  break;
            default: /* 未知区域不加分 */;
        }

        // 2. 面积段得分
        switch (areaRange) {
            case "50㎡以下":   score += 2;  break;
            case "50-90㎡":    score += 4;  break;
            case "90-144㎡":   score += 6;  break;
            case "144-236㎡":  score += 8;  break;
            case "236㎡以上":  score += 10; break;
            default: /* 未知面积段不加分 */;
        }

        // 3. 户型分类得分
        if ("小户型".equals(layoutCategory)) {
            score += 2;
        } else if ("中户型".equals(layoutCategory)) {
            score += 6;
        } else if ("大户型".equals(layoutCategory)) {
            score += 10;
        }

        // 4. 装修分类得分
        if ("精装".equals(decorationCategory)) {
            score += 10;
        } else if ("其它".equals(decorationCategory)) {
            score += 6;
        } else if ("简装".equals(decorationCategory)) {
            score += 5;
        } else if ("毛坯".equals(decorationCategory)) {
            score += 2;
        }

        // 5. 电梯得分
        if (elevatorInt != null && elevatorInt == 1) {
            score += 2;
        }

        return new IntWritable(score);
    }
}