package com.qdu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class CalculateHouseAgeUDF extends UDF {

    private static final int CURRENT_YEAR = 2025;

    public IntWritable evaluate(Text buildYearStr) {
        if (buildYearStr == null || buildYearStr.toString().trim().isEmpty()) {
            return null;
        }
        try {
            int year = Integer.parseInt(buildYearStr.toString().trim());
            if (year <= 0 || year > CURRENT_YEAR) return null;
            return new IntWritable(CURRENT_YEAR - year);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public IntWritable evaluate(String buildYearStr) {
        return evaluate(buildYearStr == null ? null : new Text(buildYearStr));
    }
}