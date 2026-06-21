import pandas as pd
import os

print("验证清洗后的数据中是否存在NaT/NaN值...")

# 检查所有输出文件
output_files = [
    'cleaned/crime_cleaned.csv',
    'cleaned/crime_with_versionid.csv',
    'versions/crime_version1.csv',
    'versions/crime_version2.csv',
    'versions/crime_version3.csv'
]

for file_path in output_files:
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        continue
    
    print(f"\n检查文件: {file_path}")
    df = pd.read_csv(file_path, keep_default_na=False)
    
    # 检查日期字段
    date_columns = ['dispatch_time', 'start_time', 'end_time']
    for col in date_columns:
        if col in df.columns:
            # 检查NaN值
            nan_count = df[col].isna().sum()
            # 检查空字符串
            empty_count = df[col].eq('').sum()
            # 检查dtype
            dtype = df[col].dtype
            
            print(f"  {col}:")
            print(f"    - {nan_count}个NaN值")
            print(f"    - {empty_count}个空字符串")
            print(f"    - 数据类型: {dtype}")
            
            # 如果是object类型，检查是否有无效字符
            if dtype == 'object':
                # 检查是否包含非日期时间字符（除了数字、-、:、空格）
                invalid_chars = df[col].str.match(r'[^d\-:\s]', na=False)
                invalid_count = invalid_chars.sum()
                if invalid_count > 0:
                    print(f"    - {invalid_count}个包含无效字符")
    
    # 检查所有列是否有NaN值
    total_nan = df.isna().sum().sum()
    print(f"\n  整个文件总NaN值: {total_nan}")
    
    # 打印数据样例
    if len(df) > 0:
        print(f"\n  数据样例 (前5行日期字段):")
        print(df[date_columns].head())

print("\n验证完成！")