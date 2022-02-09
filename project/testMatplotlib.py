#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testMatplotlib.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 18, 2021
# ---

import datetime
import numpy as np
import pandas as pd
import pandas
import sqlalchemy
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates
import matplotlib.ticker as ticker
from PIL.Image import Image
from matplotlib import pyplot as plt


def conPlotData():  # 链接PG数据库，查询已处理的时段
    querySql = """select top (15)
                        日期,
                        sum(总数) as 总数,
                        avg(漏失率) as 漏失率,
                        sum(漏失数量) as 漏失数量
                    from actiontracker_data
                    group by 日期
                    order by 日期 desc
                    """  # 趋势图
    querySql2 = """select top (5) 站别,
                           sum(漏失数量) as 漏失数量
                    from actiontracker_data
                    group by 站别
                    order by 漏失数量 desc
                    """  # 月度柏拉图
    querySql3 = """select top (5) 站别,
                            sum(漏失数量) as 漏失数量,
                            LEFT(CONVERT(NVARCHAR(40),DATEADD(day , -1, GETDATE()),120),10)+' 00:00:00' as 日期
                        from actiontracker_data
                        where 日期=LEFT(CONVERT(NVARCHAR(40),DATEADD(day , -1, GETDATE()),120),10)+' 00:00:00'
                        group by 站别
                        order by 漏失数量 desc
                    """  # 天柏拉图
    db_url = 'mssql+pymssql://PBIuser:PBIUser123456@CNWXIM0TRSQLV4A/Metal_Robot'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(querySql)
    row = cursor.fetchall()
    # row =  [''.join(i) for i in row]    #元组转化为列表
    df = pandas.DataFrame(row)
    df.columns = [
        '日期',
        '总数',
        '漏失率',
        '漏失数量']
    cursor.execute(querySql2)
    row = cursor.fetchall()
    df1 = pandas.DataFrame(row)
    df1.columns = [
        '站别',
        '漏失数量']
    cursor.execute(querySql3)
    row = cursor.fetchall()
    df2 = pandas.DataFrame(row)
    df2.columns = [
        '站别',
        '漏失数量',
        '日期']
    connection.commit()
    cursor.close()
    print('------------')
    print("数据获取完毕~")
    return [df, df1, df2]


def createPlot():
    dfs = conPlotData()
    matplotlib.rc("font", family='YouYuan', weight="bold")  # 设置字体,显示中文
    # plt.rcParams['font.sans-serif'] = ['SimHei']  # 中文乱码
    # plt.rcParams['axes.unicode_minus'] = False  # 正负号
    backgroundColor = 'white'
    barColor = '#36413e'
    lineColor = '#7d6d83'
    titleColor = 'black'
    df, df1, df2 = dfs[0], dfs[1], dfs[2]
    x = df.iloc[0:, 0]
    y1 = df.iloc[0:, 1]
    y2 = df.iloc[0:, 2] * 100
    # fig, axs = plt.subplots(constrained_layout=False, figsize=(20.16, 14.4),
    #                        dpi=80)  # 设置画布  # 像素（1080， 720）= figsize*dpi
    fig = plt.figure(figsize=(15, 10), dpi=200, facecolor=backgroundColor, edgecolor='red')   # 设置画布  # 像素（1080， 720）= figsize*dpi
    ax1 = fig.add_subplot(212)  # 设置图标位置
    rect = ax1.patch        # 设置背景色
    rect.set_facecolor(backgroundColor)
    ax1.bar(x, y1, color=barColor, width=0.8, bottom=300000, align='center')  # 柱形图
    ax1.set_ylabel('总数(million)', fontsize=16, color=titleColor)
    ax2 = ax1.twinx()
    ax2.plot(x, y2, color=lineColor)  # 折线图
    ax2.set_ylabel('漏失率(%)', fontsize=16, color=titleColor)  # 设置标签
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(1))  # 设置刻度间隔
    for tick in ax1.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(15)
    for a, b in zip(x, y1):  # 设置数字标签**
        ax1.text(a, b+300000, '%.0f' % b, ha='center', va='bottom', fontsize=10)    # a, b 为标签位置

    ax3 = fig.add_subplot(221)
    rect = ax3.patch  # 设置背景色
    rect.set_facecolor(backgroundColor)
    total = df1.agg('sum')
    total = pd.to_numeric(total.iloc[1])
    x = df1.iloc[0:, 0]
    y = df1.iloc[0:, 1]
    rat = df1.iloc[0:, 1] / total
    y2 = []
    j = 0
    for i in range(len(rat)):
        j += rat.iloc[i]
        y2.append(j)
    ax3.bar(x, y, color=barColor, width=0.8, bottom=500, align='center')  # 柱形图
    ax3.set_ylabel('漏失数量(Last 15 Days)', fontsize=16, color=titleColor)
    ax4 = ax3.twinx()
    ax4.plot(x, y2, color=lineColor)  # 折线图
    ax4.set_ylim([0, 1.1])
    for tick in ax3.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(7.5)
    # for a, b in zip(x1, y):  # 设置数字标签**
    #     ax1.text(a, b, '%.0f' % a, ha='center', va='bottom', fontsize=10)  # a, b 为标签位置

    ax5 = fig.add_subplot(222)
    rect = ax5.patch  # 设置背景色
    rect.set_facecolor(backgroundColor)
    total = df2.agg('sum')
    total = pd.to_numeric(total.iloc[1])
    x = df2.iloc[0:, 0]
    y = df2.iloc[0:, 1]
    rat = df2.iloc[0:, 1] / total
    y2 = []
    j = 0
    for i in range(len(rat)):
        j += rat.iloc[i]
        y2.append(j)
    ax5.bar(x, y, color=barColor, width=0.8, bottom=0, align='center')  # 柱形图
    ax5.set_ylabel('漏失数量(Last Day)', fontsize=16, color=titleColor)
    ax6 = ax5.twinx()
    ax6.plot(x, y2, color=lineColor)  # 折线图
    ax6.set_ylim([0, 1.1])
    for tick in ax5.get_xticklabels():  # 刻度标签旋转
        tick.set_rotation(7.5)
    # for a, b in zip(x, y):                                      # 设置数字标签**
    #     ax1.text(a, b+100, '%.0f' % b, ha='center', va='bottom', fontsize=10)    # a, b 为标签位置

    plt.savefig("C:/Users\Administrator\Desktop/1.png", bbox_inches='tight')
    plt.show()

    return


# def create_plot():
#     # 计算正弦曲线上点的 x 和 y 坐标
#     x = np.arange(0, 3 * np.pi, 0.1)
#     y = np.sin(x)
#     plt.title("sine wave form")
#     # 使用 matplotlib 来绘制点
#     plt.plot(x, y)
#     plt.show()
#
#
# def create_subplot():
#     # 计算正弦和余弦曲线上的点的 x 和 y 坐标
#     x = np.arange(0, 3 * np.pi, 0.1)
#     y_sin = np.sin(x)
#     y_cos = np.cos(x)
#     # 建立 subplot 网格，高为 2，宽为 1
#     # 激活第一个 subplot
#     plt.subplot(2, 1, 1)
#     # 绘制第一个图像
#     plt.plot(x, y_sin)
#     plt.title('Sine')
#     # 将第二个 subplot 激活，并绘制第二个图像
#     plt.subplot(2, 1, 2)
#     plt.plot(x, y_cos)
#     plt.title('Cosine')
#     # 展示图像
#     plt.show()
#
#
# def create_hist():
#     a = np.array([22, 87, 5, 43, 56, 73, 55, 54, 11, 20, 51, 5, 79, 31, 27])
#     plt.hist(a, bins=[0, 20, 40, 60, 80, 100])
#     plt.title("histogram")
#     plt.show()


if __name__ == '__main__':
    createPlot()
