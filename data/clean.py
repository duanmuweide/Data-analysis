import pandas as pd
import numpy as np

# 读取数据
df = pd.read_csv('Crime.csv')

# 1. 日期字段格式化
date_columns = ['Dispatch Date / Time', 'Start_Date_Time', 'End_Date_Time']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

# 2. 经纬度处理
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
# 标记 (0,0) 为无效
df.loc[(df['Latitude'] == 0) & (df['Longitude'] == 0), ['Latitude', 'Longitude']] = np.nan

# 3. 处理缺失值（标记为 NULL 或填充）
df.fillna({'City': 'UNKNOWN', 'State': 'MD'}, inplace=True)

# 4. 去除重复记录（基于 Incident ID）
df.drop_duplicates(subset=['Incident ID'], inplace=True)

# 5. 标准化 City 字段（统一大小写）
df['City'] = df['City'].str.strip().str.title()

# 6. 检测日期逻辑异常
df['Start_Date_Time'] = pd.to_datetime(df['Start_Date_Time'])
df['End_Date_Time'] = pd.to_datetime(df['End_Date_Time'])
df['Date_Valid'] = df['End_Date_Time'] >= df['Start_Date_Time']

# 7. 保存清洗后的数据
df.to_csv('crime_cleaned.csv', index=False)
print("数据清洗完成，保存为 crime_cleaned.csv")