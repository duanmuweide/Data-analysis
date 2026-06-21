package com.qdu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class WeightedPriceUDF extends UDF {
    public IntWritable evaluate(IntWritable districtPrice, IntWritable areaPrice, IntWritable housePrice) {
        if (districtPrice == null || areaPrice == null || housePrice == null) {
            return null;
        }
        double weighted =
                districtPrice.get() * 0.1 +
                        areaPrice.get() * 0.3 +
                        housePrice.get() * 0.6;
        return new IntWritable((int) Math.round(weighted));
    }
}
