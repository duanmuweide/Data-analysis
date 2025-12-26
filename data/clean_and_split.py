import pandas as pd
import numpy as np
import os

# 确保输出目录存在
os.makedirs('cleaned', exist_ok=True)
os.makedirs('versions', exist_ok=True)

print("开始数据清洗和分割...")

# 1. 读取原始数据
print("读取原始数据文件 crime_raw.csv...")
df = pd.read_csv('crime_raw.csv', dtype=str)

print(f"原始数据行数: {len(df)}")

# 2. 数据清洗逻辑
print("开始数据清洗...")

# 2.1 处理日期字段
date_columns = ['Dispatch Date / Time', 'Start_Date_Time', 'End_Date_Time']
for col in date_columns:
    # 确保列是字符串类型
    df[col] = df[col].astype(str).fillna('')
    # 处理空字符串和无效值
    df[col] = df[col].replace(['', 'nan'], '')
    # 将字符串转换为datetime，错误值设为NaT
    df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
    # 格式化日期为标准格式，NaT转换为空字符串
    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    # 确保所有NaT都被转换为空字符串
    df[col] = df[col].fillna('')
    # 强制转换为字符串类型
    df[col] = df[col].astype(str)
    # 再次替换任何可能的NaN或NaT
    df[col] = df[col].replace(['nan', 'NaT'], '')

# 2.2 处理数值字段
numeric_columns = ['Offence Code', 'Victims', 'Zip Code', 'Latitude', 'Longitude', 'Address Number']
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 2.3 处理经纬度无效值
# 标记 (0,0) 或缺失为无效
df.loc[(df['Latitude'] == 0) & (df['Longitude'] == 0), ['Latitude', 'Longitude']] = np.nan
# 验证经纬度范围（基于真实地理坐标范围）
df.loc[(df['Latitude'] < -90) | (df['Latitude'] > 90), 'Latitude'] = np.nan
df.loc[(df['Longitude'] < -180) | (df['Longitude'] > 180), 'Longitude'] = np.nan

# 2.4 处理缺失值
# 对不同类型的字段使用不同的缺失值处理方式
# 文本字段用空字符串填充
text_columns = ['City', 'State', 'Street Name', 'Street Type', 'Police District Name', 'Place', 'Block Address', 'Street Prefix', 'Street Suffix']
for col in text_columns:
    df[col] = df[col].fillna('')
    df[col] = df[col].astype(str)

# 日期字段确保是字符串类型且没有NaN值
date_columns = ['Dispatch Date / Time', 'Start_Date_Time', 'End_Date_Time']
for col in date_columns:
    df[col] = df[col].fillna('')
    df[col] = df[col].astype(str)
    df[col] = df[col].replace(['nan', 'NaT'], '')

# 数值字段暂时保留NaN值，后续会根据业务规则处理
# 注意：在保存到CSV时，pandas会将NaN转换为空字符串

# 2.5 标准化文本字段
text_columns = ['City', 'State', 'Street Name', 'Street Type', 'Police District Name', 'Place', 'Block Address']
for col in text_columns:
    df[col] = df[col].str.strip().str.title().fillna('')

# 2.6 严格验证State字段（只允许有效的州代码）
valid_states = ['VA', 'DC', 'MD']  # 根据数据实际情况调整
df['State'] = df['State'].str.upper()
df.loc[~df['State'].isin(valid_states + ['']), 'State'] = ''

# 2.7 验证City字段长度
max_city_length = 50
df.loc[df['City'].str.len() > max_city_length, 'City'] = ''

# 2.8 验证Zip Code格式（5位数字）
df['Zip Code'] = pd.to_numeric(df['Zip Code'], errors='coerce')
df.loc[(df['Zip Code'] < 10000) | (df['Zip Code'] > 99999), 'Zip Code'] = np.nan

# 2.9 验证Offence Code格式
df['Offence Code'] = pd.to_numeric(df['Offence Code'], errors='coerce')
df.loc[(df['Offence Code'] < 1000) | (df['Offence Code'] > 9999), 'Offence Code'] = np.nan

# 2.10 验证Victims数量（不允许超过100）
df['Victims'] = pd.to_numeric(df['Victims'], errors='coerce')
df.loc[df['Victims'] > 100, 'Victims'] = np.nan

# 2.11 去除重复记录
# 基于Incident ID去除重复
df.drop_duplicates(subset=['Incident ID'], inplace=True)
# 基于多个关键字段去除潜在重复记录
df.drop_duplicates(subset=['Incident ID', 'Start_Date_Time', 'Offence Code', 'Latitude', 'Longitude'], inplace=True)

# 2.12 处理时间逻辑
# 确保结束时间不早于开始时间
start_time_valid = df['Start_Date_Time'] != ''
end_time_valid = df['End_Date_Time'] != ''
valid_time_order = (~start_time_valid | ~end_time_valid) | (df['End_Date_Time'] >= df['Start_Date_Time'])
df = df[valid_time_order]

# 2.13 验证地址信息完整性
# 至少需要有街道名称或街区地址
has_address = (df['Block Address'] != '') | (df['Street Name'] != '')
df = df[has_address]

# 2.14 删除无效记录
# 删除没有Start_Date_Time的记录
df = df[df['Start_Date_Time'] != '']

# 删除Offence Code为空的记录
df = df[df['Offence Code'] != '']

# 删除Victims为0或空或大于100的记录
df = df[df['Victims'] != 0]
df = df[df['Victims'] != '']

# 删除经纬度都为空的记录
has_geo = (df['Latitude'] != '') | (df['Longitude'] != '')
df = df[has_geo]

# 删除没有城市信息的记录
df = df[df['City'] != '']

# 删除没有州信息的记录
df = df[df['State'] != '']

# 删除没有NIBRS Code的记录
df = df[df['NIBRS Code'] != '']

# 删除没有Police District Name的记录
df = df[df['Police District Name'] != '']

# 2.15 重命名列以匹配Hive表结构
df.columns = [
    'incident_id', 'offence_code', 'cr_number', 'dispatch_time', 'start_time', 'end_time',
    'nibrs_code', 'victims', 'crime_name1', 'crime_name2', 'crime_name3', 'police_district',
    'block_address', 'city', 'state', 'zip_code', 'agency', 'place', 'sector', 'beat',
    'pra', 'address_num', 'street_prefix', 'street_name', 'street_suffix', 'street_type',
    'latitude', 'longitude', 'district_num', 'location'
]

print(f"清洗后数据行数: {len(df)}")

# 3. 将数据分割成3个版本
print("开始分割数据为3个版本...")

# 根据incident_id排序
df.sort_values('incident_id', inplace=True)

# 计算每个版本的数据量
total_rows = len(df)
version_size = total_rows // 3

# 分割数据
version1 = df.iloc[:version_size].copy()
version2 = df.iloc[version_size:2*version_size].copy()
version3 = df.iloc[2*version_size:].copy()

# 添加版本ID
version1['versionid'] = '1'
version2['versionid'] = '2'
version3['versionid'] = '3'

print(f"版本1数据行数: {len(version1)}")
print(f"版本2数据行数: {len(version2)}")
print(f"版本3数据行数: {len(version3)}")

# 4. 保存数据
print("保存清洗后的数据文件...")

# 保存完整清洗后的数据
df.to_csv('cleaned/crime_cleaned.csv', index=False)

# 保存带版本ID的完整数据
df_with_version = pd.concat([version1, version2, version3])
df_with_version.to_csv('cleaned/crime_with_versionid.csv', index=False)

# 保存3个版本文件
version1.to_csv('versions/crime_version1.csv', index=False)
version2.to_csv('versions/crime_version2.csv', index=False)
version3.to_csv('versions/crime_version3.csv', index=False)

print("\n数据清洗和分割完成！")
print(f"- 清洗后的数据: cleaned/crime_cleaned.csv")
print(f"- 带版本ID的数据: cleaned/crime_with_versionid.csv")
print(f"- 版本1数据: versions/crime_version1.csv")
print(f"- 版本2数据: versions/crime_version2.csv")
print(f"- 版本3数据: versions/crime_version3.csv")
