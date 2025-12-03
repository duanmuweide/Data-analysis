import pandas as pd
import numpy as np

def clean_watershed_data(input_file="CountyNNIkgsqkm.csv", output_file="CleanedCountyNNIkgsqkm.csv"):
    # 读取CSV文件
    df = pd.read_csv(input_file)
    
    # 查看数据基本信息
    print("原始数据形状:", df.shape)
    print("各列NA值数量:\n", df.isnull().sum())
    
    # 处理NA值：数值型列用中位数填充，非数值型列（如FIPS）保留原数据（若有NA则删除该行）
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            # 计算该列中位数（忽略NA）
            median_val = df[col].median()
            # 用中位数填充NA
            df[col].fillna(median_val, inplace=True)
            print(f"列 {col} 用中位数 {median_val:.4f} 填充NA值")
    
    # 处理非数值型列的NA（若存在则删除该行，FIPS为标识列不填充）
    non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
    for col in non_numeric_cols:
        initial_count = df.shape[0]
        df.dropna(subset=[col], inplace=True)
        final_count = df.shape[0]
        if initial_count != final_count:
            print(f"列 {col} 删除了 {initial_count - final_count} 条含NA的记录")
    
    # 验证清洗结果
    print("\n清洗后NA值数量:\n", df.isnull().sum())
    print("清洗后数据形状:", df.shape)
    
    # 将清洗后的数据写入新CSV文件
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"\n清洗完成!结果已保存至 {output_file}")

if __name__ == "__main__":
    clean_watershed_data(
        input_file="CountyNNIkgsqkm.csv",
        output_file="CleanedCountyNNIkgsqkm.csv"
    )