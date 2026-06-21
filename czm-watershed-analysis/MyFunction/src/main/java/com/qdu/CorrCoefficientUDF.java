package com.qdu;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Hive自定义UDF：计算两组数值的皮尔逊相关系数
 * 输入：农业氮盈余列表、大气氮沉降列表
 * 输出：皮尔逊相关系数（字符串格式，保留4位小数）
 */
public class CorrCoefficientUDF extends UDF {

  /**
   * 核心方法：Hive UDF入口（evaluate方法名固定）
   * @param surplusList 农业氮盈余数值列表（n_ag_surplus_kgsqkm）
   * @param depList 大气氮沉降数值列表（n_atm_dep_kgsqkm）
   * @return 皮尔逊相关系数（NULL表示计算失败）
   */
  public Text evaluate(List<Double> surplusList, List<Double> depList) {
    // 1. 入参校验
    if (surplusList == null || depList == null) {
      return new Text("NULL");
    }
    int dataSize = surplusList.size();
    if (dataSize == 0 || dataSize != depList.size()) {
      return new Text("NULL");
    }
    // 过滤空值
    double[] surplusArr = new double[dataSize];
    double[] depArr = new double[dataSize];
    int validCount = 0;
    for (int i = 0; i < dataSize; i++) {
      Double s = surplusList.get(i);
      Double d = depList.get(i);
      if (s == null || d == null || s.isNaN() || d.isNaN()) {
        continue;
      }
      surplusArr[validCount] = s;
      depArr[validCount] = d;
      validCount++;
    }
    // 有效数据不足2个，无法计算相关性
    if (validCount < 2) {
      return new Text("NULL");
    }

    // 2. 计算皮尔逊相关系数
    double corr = calculatePearsonCorrelation(surplusArr, depArr, validCount);

    // 3. 返回格式化结果（保留4位小数）
    return new Text(String.format("%.4f", corr));
  }

  /**
   * 计算皮尔逊相关系数核心逻辑
   * 公式：r = [Σ((Xi - X̄)(Yi - Ȳ))] / √[Σ(Xi - X̄)² * Σ(Yi - Ȳ)²]
   */
  private double calculatePearsonCorrelation(double[] x, double[] y, int count) {
    // 步骤1：计算均值
    double xMean = 0.0, yMean = 0.0;
    for (int i = 0; i < count; i++) {
      xMean += x[i];
      yMean += y[i];
    }
    xMean /= count;
    yMean /= count;

    // 步骤2：计算分子（协方差和）、分母（方差和）
    double numerator = 0.0; // 分子：Σ((Xi - X̄)(Yi - Ȳ))
    double xDenominator = 0.0; // X方差和：Σ(Xi - X̄)²
    double yDenominator = 0.0; // Y方差和：Σ(Yi - Ȳ)²
    for (int i = 0; i < count; i++) {
      double xDiff = x[i] - xMean;
      double yDiff = y[i] - yMean;

      numerator += xDiff * yDiff;
      xDenominator += xDiff * xDiff;
      yDenominator += yDiff * yDiff;
    }

    // 步骤3：避免除0
    if (xDenominator == 0 || yDenominator == 0) {
      return 0.0;
    }

    // 步骤4：计算相关系数
    return numerator / Math.sqrt(xDenominator * yDenominator);
  }
}
