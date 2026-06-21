//package com.qdu;
//
//import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.Text;
//
//// 自定义函数：计算氮磷综合污染风险等级
//public class PollutantRiskLevelUDF extends UDF {
//  // 核心方法：输入氮盈余、磷盈余，返回风险等级
//  public Text evaluate(Double nSurplus, Double pSurplus) {
//    // 处理空值（返回"未知"）
//    if (nSurplus == null || pSurplus == null) {
//      return new Text("未知");
//    }
//
//    // 风险等级判断（逻辑简单，无复杂计算）
//    if (nSurplus <= 50 && pSurplus <= 20) {
//      return new Text("低风险");
//    } else if ((nSurplus <= 100 && nSurplus > 50) || (pSurplus <= 50 && pSurplus > 20)) {
//      return new Text("中风险");
//    } else if ((nSurplus <= 200 && nSurplus > 100) || (pSurplus <= 100 && pSurplus > 50)) {
//      return new Text("高风险");
//    } else {
//      return new Text("极高风险");
//    }
//  }
//}
