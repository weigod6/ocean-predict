

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def plot_global_temperature(csv_file_path):
    # 读取 gzipped CSV 文件
    df = pd.read_csv(csv_file_path, header=None,low_memory=False)

    # 获取纬度和经度
    lat = df.index.values.astype(float)
    lon = df.columns.values.astype(float)

    # 将 'N' 替换为 NaN
    df.replace('N', np.nan, inplace=True)

    # 将数据转换为浮点数
    data = df.values.astype(float)

    # 设置颜色映射范围
    vmin = -10  # 最小温度值，可以根据实际数据范围调整
    vmax = 35  # 最大温度值，可以根据实际数据范围调整

    # 准备绘图
    plt.figure(figsize=(12, 6))


    # 创建图像
    cmap = plt.cm.coolwarm
    cmap.set_bad(color='black')  # 设置缺失值为黑色

    # 使用imshow绘制图像，设置颜色映射范围
    plt.imshow(data, origin='lower', cmap=cmap, vmin=vmin, vmax=vmax, interpolation='none')

    # 添加颜色条
    plt.colorbar(label='Sea Surface Temperature (°C)')

    # 添加标题
    plt.title(f'Sea Surface Temperature on Day {1}')

    # 保存或显示图像
    #plt.savefig(f'{csv_file_path}.png')
    plt.ion()
    plt.pause(1)
    # 关闭当前图像以节省内存
    plt.close()
    # 创建绘图

# 示例调用
csv_file_path1 = 'output_csv/2024-01-03.csv'
csv_file_path2 = 'output_csv/2024-02-17.csv'
csv_file_path3 = 'output_csv/2024-05-25.csv'

plot_global_temperature(csv_file_path1)
plot_global_temperature(csv_file_path2)
plot_global_temperature(csv_file_path3)
