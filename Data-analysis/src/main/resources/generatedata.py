#用python来生成一百万行数据
import pandas as pd
import numpy as np
import random

# 设置随机种子（可复现）
np.random.seed(42)
random.seed(42)

NUM_ROWS = 1_000_000  # 100万行

# 定义字段取值
DISTRICTS = [
    "东城", "西城", "海淀", "朝阳", "丰台", "门头沟", "石景山",
    "房山", "通州", "顺义", "昌平", "大兴", "怀柔", "平谷", "延庆", "密云"
]

HOUSE_TYPES = [
    "1室0厅", "1室1厅", "2室1厅", "3室1厅", "3室2厅",
    "4室1厅", "4室2厅", "5室1厅", "5室2厅", "5室3厅", "别墅"
]

ORIENTATIONS = ["东", "南", "西", "北", "东南", "东北", "西南", "西北"]
DECORATION = ["精装", "简装", "其它", "毛坯"]
ELEVATOR = ["有电梯", "无电梯"]

def generate_community_names(n):
    prefixes = ["阳光", "幸福", "金地", "万科", "龙湖", "保利", "中海", "绿城", "华润", "首开",
                "远洋", "招商", "金融街", "首创", "富力", "恒大", "碧桂园", "世茂", "合生", "中信"]
    suffixes = ["家园", "小区", "花园", "苑", "府", "城", "国际", "壹号院", "公馆", "名居"]
    return [random.choice(prefixes) + random.choice(suffixes) for _ in range(n)]

print("正在生成小区名称...")
community_list = generate_community_names(NUM_ROWS)

print("正在生成结构化数据...")
data = {
    "市区": np.random.choice(DISTRICTS, size=NUM_ROWS),
    "小区": community_list,
    "户型": np.random.choice(HOUSE_TYPES, size=NUM_ROWS),
    "朝向": np.random.choice(ORIENTATIONS, size=NUM_ROWS),
    "楼层": np.random.randint(1, 33, size=NUM_ROWS),          # 1~32
    "装修情况": np.random.choice(DECORATION, size=NUM_ROWS),
    "电梯": np.random.choice(ELEVATOR, size=NUM_ROWS),
    "面积(㎡)": np.random.randint(50, 301, size=NUM_ROWS),   # 整数，50~300（含）
    "价格(万元)": np.random.randint(100, 1001, size=NUM_ROWS),  # 整数，100~1000（含）
    "年份": np.random.randint(1951, 2020, size=NUM_ROWS)     # 1951~2019（含）
}

print("正在构建 DataFrame...")
df = pd.DataFrame(data)

print("正在打乱数据顺序...")
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

print("正在写入 data.csv 文件")
df.to_csv("data.csv", index=False, encoding="utf-8-sig")

print(f"✅ 成功生成 {len(df):,} 行数据，已保存至当前目录下的 data.csv")