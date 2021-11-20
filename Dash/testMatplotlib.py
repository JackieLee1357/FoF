#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testMatplotlib.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 06, 2021
# ---
# https://mp.weixin.qq.com/s/RWA3Of5D_jti8WpNzCgXvA

import matplotlib.pyplot as plt
import seaborn as sns


def violinPlot():
    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)
    # 绘图显示
    sns.violinplot(x=df["species"], y=df["sepal_length"])
    plt.show()


def kdePlot():
    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 绘图显示
    sns.kdeplot(df['sepal_width'])
    plt.show()


def distPlot():
    import seaborn as sns
    import matplotlib.pyplot as plt

    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 绘图显示
    sns.distplot(a=df["sepal_length"], hist=True, kde=False, rug=False)
    plt.show()


def boxplot():
    import seaborn as sns
    import matplotlib.pyplot as plt

    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 绘图显示
    sns.boxplot(x=df["species"], y=df["sepal_length"])
    plt.show()


def add_annotation():
    import plotly.graph_objects as go
    import numpy as np
    import pandas as pd

    # 读取数据
    temp = pd.read_csv('2016-weather-data-seattle.csv')
    # 数据处理, 时间格式转换
    temp['year'] = pd.to_datetime(temp['Date']).dt.year

    # 选择几年的数据展示即可
    year_list = [1950, 1960, 1970, 1980, 1990, 2000, 2010]
    temp = temp[temp['year'].isin(year_list)]

    # 绘制每年的直方图，以年和平均温度分组，并使用'count'函数进行汇总
    temp = temp.groupby(['year', 'Mean_TemperatureC']).agg({'Mean_TemperatureC': 'count'}).rename(
        columns={'Mean_TemperatureC': 'count'}).reset_index()

    # 使用Plotly绘制脊线图，每个轨迹对应于特定年份的温度分布
    # 将每年的数据(温度和它们各自的计数)存储在单独的数组，并将其存储在字典中以方便检索
    array_dict = {}
    for year in year_list:
        # 每年平均温度
        array_dict[f'x_{year}'] = temp[temp['year'] == year]['Mean_TemperatureC']
        # 每年温度计数
        array_dict[f'y_{year}'] = temp[temp['year'] == year]['count']
        array_dict[f'y_{year}'] = (array_dict[f'y_{year}'] - array_dict[f'y_{year}'].min()) \
                                  / (array_dict[f'y_{year}'].max() - array_dict[f'y_{year}'].min())

    # 创建一个图像对象
    fig = go.Figure()
    for index, year in enumerate(year_list):
        # 使用add_trace()绘制轨迹
        fig.add_trace(go.Scatter(
            x=[-20, 40], y=np.full(2, len(year_list) - index),
            mode='lines',
            line_color='white'))

        fig.add_trace(go.Scatter(
            x=array_dict[f'x_{year}'],
            y=array_dict[f'y_{year}'] + (len(year_list) - index) + 0.4,
            fill='tonexty',
            name=f'{year}'))

        # 添加文本
        fig.add_annotation(
            x=-20,
            y=len(year_list) - index,
            text=f'{year}',
            showarrow=False,
            yshift=10)

    # 添加标题、图例、xy轴参数
    fig.update_layout(
        title='1950年～2010年西雅图平均温度',
        showlegend=False,
        xaxis=dict(title='单位: 摄氏度'),
        yaxis=dict(showticklabels=False)
    )

    # 跳转网页显示
    fig.show()


def regPlot():
    import seaborn as sns
    import matplotlib.pyplot as plt

    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 绘图显示
    sns.regplot(x=df["sepal_length"], y=df["sepal_width"])
    plt.show()


def heatmap():
    import seaborn as sns
    import pandas as pd
    import numpy as np

    # Create a dataset
    df = pd.DataFrame(np.random.random((5, 5)), columns=["a", "b", "c", "d", "e"])

    # Default heatmap
    p1 = sns.heatmap(df)


def pairPlot():
    import seaborn as sns
    import matplotlib.pyplot as plt

    # 加载数据
    df = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 绘图显示
    sns.pairplot(df)
    plt.show()


def scatterplot():
    import matplotlib.pyplot as plt
    import seaborn as sns
    from gapminder import gapminder

    # 导入数据
    data = gapminder.loc[gapminder.year == 2007]

    # 使用scatterplot创建气泡图
    sns.scatterplot(data=data, x="gdpPercap", y="lifeExp", size="pop", legend=False, sizes=(20, 2000))

    # 显示
    plt.show()


def plot1():
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    # 创建数据
    df = pd.DataFrame({'x_axis': range(1, 10), 'y_axis': np.random.randn(9) * 80 + range(1, 10)})

    # 绘制显示
    plt.plot('x_axis', 'y_axis', data=df, linestyle='-', marker='o')
    plt.show()


def gaussian_kde():
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.stats import kde

    # 创建数据, 200个点
    data = np.random.multivariate_normal([0, 0], [[1, 0.5], [0.5, 3]], 200)
    x, y = data.T

    # 创建画布, 6个子图
    fig, axes = plt.subplots(ncols=6, nrows=1, figsize=(21, 5))

    # 第一个子图, 散点图
    axes[0].set_title('Scatterplot')
    axes[0].plot(x, y, 'ko')

    # 第二个子图, 六边形
    nbins = 20
    axes[1].set_title('Hexbin')
    axes[1].hexbin(x, y, gridsize=nbins, cmap=plt.cm.BuGn_r)

    # 2D 直方图
    axes[2].set_title('2D Histogram')
    axes[2].hist2d(x, y, bins=nbins, cmap=plt.cm.BuGn_r)

    # 高斯kde
    k = kde.gaussian_kde(data.T)
    xi, yi = np.mgrid[x.min():x.max():nbins * 1j, y.min():y.max():nbins * 1j]
    zi = k(np.vstack([xi.flatten(), yi.flatten()]))

    # 密度图
    axes[3].set_title('Calculate Gaussian KDE')
    axes[3].pcolormesh(xi, yi, zi.reshape(xi.shape), shading='auto', cmap=plt.cm.BuGn_r)

    # 添加阴影
    axes[4].set_title('2D Density with shading')
    axes[4].pcolormesh(xi, yi, zi.reshape(xi.shape), shading='gouraud', cmap=plt.cm.BuGn_r)

    # 添加轮廓
    axes[5].set_title('Contour')
    axes[5].pcolormesh(xi, yi, zi.reshape(xi.shape), shading='gouraud', cmap=plt.cm.BuGn_r)
    axes[5].contour(xi, yi, zi.reshape(xi.shape))

    plt.show()


def bar1():
    import numpy as np
    import matplotlib.pyplot as plt

    # 生成随机数据
    height = [3, 12, 5, 18, 45]
    bars = ('A', 'B', 'C', 'D', 'E')
    y_pos = np.arange(len(bars))

    # 创建条形图
    plt.bar(y_pos, height)

    # x轴标签
    plt.xticks(y_pos, bars)

    # 显示
    plt.show()


def legend():
    import matplotlib.pyplot as plt
    import pandas as pd
    from math import pi

    # 设置数据
    df = pd.DataFrame({
        'group': ['A', 'B', 'C', 'D'],
        'var1': [38, 1.5, 30, 4],
        'var2': [29, 10, 9, 34],
        'var3': [8, 39, 23, 24],
        'var4': [7, 31, 33, 14],
        'var5': [28, 15, 32, 14]
    })

    # 目标数量
    categories = list(df)[1:]
    N = len(categories)

    # 角度
    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]

    # 初始化
    ax = plt.subplot(111, polar=True)

    # 设置第一处
    ax.set_theta_offset(pi / 2)
    ax.set_theta_direction(-1)

    # 添加背景信息
    plt.xticks(angles[:-1], categories)
    ax.set_rlabel_position(0)
    plt.yticks([10, 20, 30], ["10", "20", "30"], color="grey", size=7)
    plt.ylim(0, 40)

    # 添加数据图

    # 第一个
    values = df.loc[0].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=1, linestyle='solid', label="group A")
    ax.fill(angles, values, 'b', alpha=0.1)

    # 第二个
    values = df.loc[1].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=1, linestyle='solid', label="group B")
    ax.fill(angles, values, 'r', alpha=0.1)

    # 添加图例
    plt.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))

    # 显示
    plt.show()


def wordcloud():
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt

    # 添加词语
    text = ("Python Python Python Matplotlib Chart Wordcloud Boxplot")

    # 创建词云对象
    wordcloud = WordCloud(width=480, height=480, margin=0).generate(text)

    # 显示词云图
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.margins(x=0, y=0)
    plt.show()


def parallel_coordinates():
    import seaborn as sns
    import matplotlib.pyplot as plt
    from pandas.plotting import parallel_coordinates

    # 读取数据
    data = sns.load_dataset('iris', data_home='seaborn-data', cache=True)

    # 创建图表
    parallel_coordinates(data, 'species', colormap=plt.get_cmap("Set2"))

    # 显示
    plt.show()


def stem():
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np

    # 创建数据
    df = pd.DataFrame({'group': list(map(chr, range(65, 85))), 'values': np.random.uniform(size=20)})

    # 排序取值
    ordered_df = df.sort_values(by='values')
    my_range = range(1, len(df.index) + 1)

    # 创建图表
    plt.stem(ordered_df['values'])
    plt.xticks(my_range, ordered_df['group'])

    # 显示
    plt.show()


def slope():
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np

    # 生成数据
    df = pd.DataFrame(
        {
            'Name': ['item ' + str(i) for i in list(range(1, 51))],
            'Value': np.random.randint(low=10, high=100, size=50)
        })

    # 排序
    df = df.sort_values(by=['Value'])

    # 初始化画布
    plt.figure(figsize=(20, 10))
    ax = plt.subplot(111, polar=True)
    plt.axis('off')

    # 设置图表参数
    upperLimit = 100
    lowerLimit = 30
    labelPadding = 4

    # 计算最大值
    max = df['Value'].max()

    # 数据下限10, 上限100
    slope = (max - lowerLimit) / max
    heights = slope * df.Value + lowerLimit

    # 计算条形图的宽度
    width = 2 * np.pi / len(df.index)

    # 计算角度
    indexes = list(range(1, len(df.index) + 1))
    angles = [element * width for element in indexes]

    # 绘制条形图
    bars = ax.bar(
        x=angles,
        height=heights,
        width=width,
        bottom=lowerLimit,
        linewidth=2,
        edgecolor="white",
        color="#61a4b2",
    )

    # 添加标签
    for bar, angle, height, label in zip(bars, angles, heights, df["Name"]):

        # 旋转
        rotation = np.rad2deg(angle)

        # 翻转
        alignment = ""
        if np.pi / 2 <= angle < 3 * np.pi / 2:
            alignment = "right"
            rotation = rotation + 180
        else:
            alignment = "left"

        # 最后添加标签
        ax.text(
            x=angle,
            y=lowerLimit + bar.get_height() + labelPadding,
            s=label,
            ha=alignment,
            va='center',
            rotation=rotation,
            rotation_mode="anchor")

    plt.show()


def squarify():
    import matplotlib.pyplot as plt
    import squarify
    import pandas as pd

    # 创建数据
    df = pd.DataFrame({'nb_people': [8, 3, 4, 2], 'group': ["group A", "group B", "group C", "group D"]})

    # 绘图显示
    squarify.plot(sizes=df['nb_people'], label=df['group'], alpha=.8)
    plt.axis('off')
    plt.show()


def venn2():
    import matplotlib.pyplot as plt
    from matplotlib_venn import venn2

    # 创建图表
    venn2(subsets=(10, 5, 2), set_labels=('Group A', 'Group B'))
    # 显示
    plt.show()


def gca():
    import matplotlib.pyplot as plt

    # 创建数据
    size_of_groups = [12, 11, 3, 30]

    # 生成饼图
    plt.pie(size_of_groups)

    # 在中心添加一个圆, 生成环形图
    my_circle = plt.Circle((0, 0), 0.7, color='white')
    p = plt.gcf()
    p.gca().add_artist(my_circle)

    plt.show()


def pie():
    import matplotlib.pyplot as plt

    # 创建数据
    size_of_groups = [12, 11, 3, 30]

    # 生成饼图
    plt.pie(size_of_groups)
    plt.show()


if __name__ == "__main__":
    # pie()
    # gca()
    # squarify()
    # slope()
    # stem()
    # parallel_coordinates()
    # wordcloud()
    # violinPlot()
    # kdePlot()
    # distPlot()
    # boxplot()
    # add_annotation()
    # regPlot()
    # heatmap()
    # pairPlot()
    # plot1()
    # gaussian_kde()
    # bar1()
    legend()
