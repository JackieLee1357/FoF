#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testMatplotlib2.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 11月 06, 2021
# ---
# https://mp.weixin.qq.com/s/RWA3Of5D_jti8WpNzCgXvA

def dendrogram():
    import pandas as pd
    from matplotlib import pyplot as plt
    from scipy.cluster.hierarchy import dendrogram, linkage

    # 读取数据
    df = pd.read_csv('mtcars.csv')
    df = df.set_index('model')

    # 计算每个样本之间的距离
    Z = linkage(df, 'ward')

    # 绘图
    dendrogram(Z, leaf_rotation=90, leaf_font_size=8, labels=df.index)

    # 显示
    plt.show()


def annotate():
    import circlify
    import matplotlib.pyplot as plt

    # 创建画布, 包含一个子图
    fig, ax = plt.subplots(figsize=(14, 14))

    # 标题
    ax.set_title('Repartition of the world population')

    # 移除坐标轴
    ax.axis('off')

    # 人口数据
    data = [{'id': 'World', 'datum': 6964195249, 'children': [
        {'id': "North America", 'datum': 450448697,
         'children': [
             {'id': "United States", 'datum': 308865000},
             {'id': "Mexico", 'datum': 107550697},
             {'id': "Canada", 'datum': 34033000}
         ]},
        {'id': "South America", 'datum': 278095425,
         'children': [
             {'id': "Brazil", 'datum': 192612000},
             {'id': "Colombia", 'datum': 45349000},
             {'id': "Argentina", 'datum': 40134425}
         ]},
        {'id': "Europe", 'datum': 209246682,
         'children': [
             {'id': "Germany", 'datum': 81757600},
             {'id': "France", 'datum': 65447374},
             {'id': "United Kingdom", 'datum': 62041708}
         ]},
        {'id': "Africa", 'datum': 311929000,
         'children': [
             {'id': "Nigeria", 'datum': 154729000},
             {'id': "Ethiopia", 'datum': 79221000},
             {'id': "Egypt", 'datum': 77979000}
         ]},
        {'id': "Asia", 'datum': 2745929500,
         'children': [
             {'id': "China", 'datum': 1336335000},
             {'id': "India", 'datum': 1178225000},
             {'id': "Indonesia", 'datum': 231369500}
         ]}
    ]}]

    # 使用circlify()计算, 获取圆的大小, 位置
    circles = circlify.circlify(
        data,
        show_enclosure=False,
        target_enclosure=circlify.Circle(x=0, y=0, r=1)
    )

    lim = max(
        max(
            abs(circle.x) + circle.r,
            abs(circle.y) + circle.r,
        )
        for circle in circles
    )
    plt.xlim(-lim, lim)
    plt.ylim(-lim, lim)

    for circle in circles:
        if circle.level != 2:
            continue
        x, y, r = circle
        ax.add_patch(plt.Circle((x, y), r, alpha=0.5, linewidth=2, color="lightblue"))

    for circle in circles:
        if circle.level != 3:
            continue
        x, y, r = circle
        label = circle.ex["id"]
        ax.add_patch(plt.Circle((x, y), r, alpha=0.5, linewidth=2, color="#69b3a2"))
        plt.annotate(label, (x, y), ha='center', color="white")

    for circle in circles:
        if circle.level != 2:
            continue
        x, y, r = circle
        label = circle.ex["id"]
        plt.annotate(label, (x, y), va='center', ha='center',
                     bbox=dict(facecolor='white', edgecolor='black', boxstyle='round', pad=.5))

    plt.show()


def plot():
    import matplotlib.pyplot as plt
    import numpy as np

    # 创建数据
    values = np.cumsum(np.random.randn(1000, 1))

    # 绘制图表
    plt.plot(values)
    plt.show()


def fill_between():
    import matplotlib.pyplot as plt

    # 创建数据
    x = range(1, 6)
    y = [1, 4, 6, 8, 4]

    # 生成图表
    plt.fill_between(x, y)
    plt.show()


def legend():
    import matplotlib.pyplot as plt

    # 创建数据
    x = range(1, 6)
    y1 = [1, 4, 6, 8, 9]
    y2 = [2, 2, 7, 10, 12]
    y3 = [2, 8, 5, 10, 6]

    # 生成图表
    plt.stackplot(x, y1, y2, y3, labels=['A', 'B', 'C'])
    plt.legend(loc='upper left')
    plt.show()


def stackPlot():
    import matplotlib.pyplot as plt
    import numpy as np
    from scipy import stats

    # 添加数据
    x = np.arange(1990, 2020)
    y = [np.random.randint(0, 5, size=30) for _ in range(5)]

    def gaussian_smooth(x, y, grid, sd):
        """平滑曲线"""
        weights = np.transpose([stats.norm.pdf(grid, m, sd) for m in x])
        weights = weights / weights.sum(0)
        return (weights * y).sum(1)

    # 自定义颜色
    COLORS = ["#D0D1E6", "#A6BDDB", "#74A9CF", "#2B8CBE", "#045A8D"]

    # 创建画布
    fig, ax = plt.subplots(figsize=(10, 7))

    # 生成图表
    grid = np.linspace(1985, 2025, num=500)
    y_smoothed = [gaussian_smooth(x, y_, grid, 1) for y_ in y]
    ax.stackplot(grid, y_smoothed, colors=COLORS, baseline="sym")

    # 显示
    plt.show()


def suptitle():
    import numpy as np
    import seaborn as sns
    import pandas as pd
    import matplotlib.pyplot as plt

    # 创建数据
    my_count = ["France", "Australia", "Japan", "USA", "Germany", "Congo", "China", "England", "Spain", "Greece",
                "Marocco",
                "South Africa", "Indonesia", "Peru", "Chili", "Brazil"]
    df = pd.DataFrame({
        "country": np.repeat(my_count, 10),
        "years": list(range(2000, 2010)) * 16,
        "value": np.random.rand(160)
    })

    # 创建网格
    g = sns.FacetGrid(df, col='country', hue='country', col_wrap=4, )

    # 添加曲线图
    g = g.map(plt.plot, 'years', 'value')

    # 面积图
    g = g.map(plt.fill_between, 'years', 'value', alpha=0.2).set_titles("{col_name} country")

    # 标题
    g = g.set_titles("{col_name}")

    # 总标题
    plt.subplots_adjust(top=0.92)
    g = g.fig.suptitle('Evolution of the value of stuff in 16 countries')

    # 显示
    plt.show()


def folium():
    import pandas as pd
    import folium

    # 创建地图对象
    m = folium.Map(location=[20, 0], tiles="OpenStreetMap", zoom_start=2)

    # 创建图标数据
    data = pd.DataFrame({
        'lon': [-58, 2, 145, 30.32, -4.03, -73.57, 36.82, -38.5],
        'lat': [-34, 49, -38, 59.93, 5.33, 45.52, -1.29, -12.97],
        'name': ['Buenos Aires', 'Paris', 'melbourne', 'St Petersbourg', 'Abidjan', 'Montreal', 'Nairobi', 'Salvador'],
        'value': [10, 12, 40, 70, 23, 43, 100, 43]
    }, dtype=str)

    # 添加信息
    for i in range(0, len(data)):
        folium.Marker(
            location=[data.iloc[i]['lat'], data.iloc[i]['lon']],
            popup=data.iloc[i]['name'],
        ).add_to(m)

    # 保存
    m.save('map.html')


def folium1():
    import pandas as pd
    import folium

    # 创建地图对象
    m = folium.Map(location=[40, -95], zoom_start=4)

    # 读取数据
    state_geo = f"us-states.json"
    state_unemployment = f"US_Unemployment_Oct2012.csv"
    state_data = pd.read_csv(state_unemployment)

    folium.Choropleth(
        geo_data=state_geo,
        name="choropleth",
        data=state_data,
        columns=["State", "Unemployment"],
        key_on="feature.id",
        fill_color="YlGn",
        fill_opacity=0.7,
        line_opacity=.1,
        legend_name="Unemployment Rate (%)",
    ).add_to(m)

    folium.LayerControl().add_to(m)
    # 保存
    m.save('choropleth-map.html')


def ScalarMappable():
    import pandas as pd
    import geopandas as gpd
    import matplotlib.pyplot as plt

    # 读取数据
    file = "us_states_hexgrid.geojson.json"
    geoData = gpd.read_file(file)
    geoData['centroid'] = geoData['geometry'].apply(lambda x: x.centroid)

    mariageData = pd.read_csv("State_mariage_rate.csv")
    geoData['state'] = geoData['google_name'].str.replace(' \(United States\)', '')

    geoData = geoData.set_index('state').join(mariageData.set_index('state'))

    # 初始化
    fig, ax = plt.subplots(1, figsize=(6, 4))

    # 绘图
    geoData.plot(
        ax=ax,
        column="y_2015",
        cmap="BuPu",
        norm=plt.Normalize(vmin=2, vmax=13),
        edgecolor='black',
        linewidth=.5
    )

    # 不显示坐标轴
    ax.axis('off')

    # 标题, 副标题,作者
    ax.annotate('Mariage rate in the US', xy=(10, 340), xycoords='axes pixels', horizontalalignment='left',
                verticalalignment='top', fontsize=14, color='black')
    ax.annotate('Yes, people love to get married in Vegas', xy=(10, 320), xycoords='axes pixels',
                horizontalalignment='left', verticalalignment='top', fontsize=11, color='#808080')
    ax.annotate('xiao F', xy=(400, 0), xycoords='axes pixels', horizontalalignment='left', verticalalignment='top',
                fontsize=8, color='#808080')

    # 每个网格
    for idx, row in geoData.iterrows():
        ax.annotate(
            s=row['iso3166_2'],
            xy=row['centroid'].coords[0],
            horizontalalignment='center',
            va='center',
            color="white"
        )

    # 添加颜色
    sm = plt.cm.ScalarMappable(cmap='BuPu', norm=plt.Normalize(vmin=2, vmax=13))
    fig.colorbar(sm, orientation="horizontal", aspect=50, fraction=0.005, pad=0);

    # 显示
    plt.show()


def annotate1():
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    import pandas as pd

    # 数据
    cities = {
        'city': ["Paris", "Melbourne", "Saint.Petersburg", "Abidjan", "Montreal", "Nairobi", "Salvador"],
        'lon': [2, 145, 30.32, -4.03, -73.57, 36.82, -38.5],
        'lat': [49, -38, 59.93, 5.33, 45.52, -1.29, -12.97]
    }
    df = pd.DataFrame(cities, columns=['city', 'lon', 'lat'])

    # 创建地图
    m = Basemap(llcrnrlon=-179, llcrnrlat=-60, urcrnrlon=179, urcrnrlat=70, projection='merc')
    m.drawmapboundary(fill_color='white', linewidth=0)
    m.fillcontinents(color='#f2f2f2', alpha=0.7)
    m.drawcoastlines(linewidth=0.1, color="white")

    # 循环建立连接
    for startIndex, startRow in df.iterrows():
        for endIndex in range(startIndex, len(df.index)):
            endRow = df.iloc[endIndex]
            m.drawgreatcircle(startRow.lon, startRow.lat, endRow.lon, endRow.lat, linewidth=1, color='#69b3a2');

    # 添加城市名称
    for i, row in df.iterrows():
        plt.annotate(row.city, xy=m(row.lon + 3, row.lat), verticalalignment='center')

    plt.show()


def folium1():
    import folium
    import pandas as pd

    # 创建地图对象
    m = folium.Map(location=[20, 0], tiles="OpenStreetMap", zoom_start=2)

    # 坐标点数据
    data = pd.DataFrame({
        'lon': [-58, 2, 145, 30.32, -4.03, -73.57, 36.82, -38.5],
        'lat': [-34, 49, -38, 59.93, 5.33, 45.52, -1.29, -12.97],
        'name': ['Buenos Aires', 'Paris', 'melbourne', 'St Petersbourg', 'Abidjan', 'Montreal', 'Nairobi', 'Salvador'],
        'value': [10, 12, 40, 70, 23, 43, 100, 43]
    }, dtype=str)

    # 添加气泡
    for i in range(0, len(data)):
        folium.Circle(
            location=[data.iloc[i]['lat'], data.iloc[i]['lon']],
            popup=data.iloc[i]['name'],
            radius=float(data.iloc[i]['value']) * 20000,
            color='crimson',
            fill=True,
            fill_color='crimson'
        ).add_to(m)

    # 保存
    m.save('bubble-map.html')


def Chord():
    from chord import Chord

    matrix = [
        [0, 5, 6, 4, 7, 4],
        [5, 0, 5, 4, 6, 5],
        [6, 5, 0, 4, 5, 5],
        [4, 4, 4, 0, 5, 5],
        [7, 6, 5, 5, 0, 4],
        [4, 5, 5, 5, 4, 0],
    ]

    names = ["Action", "Adventure", "Comedy", "Drama", "Fantasy", "Thriller"]

    # 保存
    Chord(matrix, names).to_html("chord-diagram.html")


def links_filtered():
    import pandas as pd
    import numpy as np
    import networkx as nx
    import matplotlib.pyplot as plt

    # 创建数据
    ind1 = [5, 10, 3, 4, 8, 10, 12, 1, 9, 4]
    ind5 = [1, 1, 13, 4, 18, 5, 2, 11, 3, 8]
    df = pd.DataFrame(
        {'A': ind1, 'B': ind1 + np.random.randint(10, size=(10)), 'C': ind1 + np.random.randint(10, size=(10)),
         'D': ind1 + np.random.randint(5, size=(10)), 'E': ind1 + np.random.randint(5, size=(10)), 'F': ind5,
         'G': ind5 + np.random.randint(5, size=(10)), 'H': ind5 + np.random.randint(5, size=(10)),
         'I': ind5 + np.random.randint(5, size=(10)), 'J': ind5 + np.random.randint(5, size=(10))})

    # 转换
    # 计算相关性
    corr = df.corr()

    links = corr.stack().reset_index()
    links.columns = ['var1', 'var2', 'value']

    # 保持相关性超过一个阈值, 删除自相关性
    links_filtered = links.loc[(links['value'] > 0.8) & (links['var1'] != links['var2'])]

    # 生成图
    G = nx.from_pandas_edgelist(links_filtered, 'var1', 'var2')

    # 绘制网络
    nx.draw(G, with_labels=True, node_color='orange', node_size=400, edge_color='black', linewidths=1, font_size=15)

    # 显示
    plt.show()


def Figure():
    import plotly.graph_objects as go
    import json

    # 读取数据
    with open('sankey_energy.json') as f:
        data = json.load(f)

    # 透明度
    opacity = 0.4
    # 颜色
    data['data'][0]['node']['color'] = ['rgba(255,0,255, 0.8)' if color == "magenta" else color for color in
                                        data['data'][0]['node']['color']]
    data['data'][0]['link']['color'] = [data['data'][0]['node']['color'][src].replace("0.8", str(opacity))
                                        for src in data['data'][0]['link']['source']]

    fig = go.Figure(data=[go.Sankey(
        valueformat=".0f",
        valuesuffix="TWh",
        # 点
        node=dict(
            pad=15,
            thickness=15,
            line=dict(color="black", width=0.5),
            label=data['data'][0]['node']['label'],
            color=data['data'][0]['node']['color']
        ),
        # 线
        link=dict(
            source=data['data'][0]['link']['source'],
            target=data['data'][0]['link']['target'],
            value=data['data'][0]['link']['value'],
            label=data['data'][0]['link']['label'],
            color=data['data'][0]['link']['color']
        ))])

    fig.update_layout(
        title_text="Energy forecast for 2050<br>Source: Department of Energy & Climate Change, Tom Counsell via <a href='https://bost.ocks.org/mike/sankey/'>Mike Bostock</a>",
        font_size=10)

    # 保持
    fig.write_html("sankey-diagram.html")


def scatter():
    import imageio
    import pandas as pd
    import matplotlib.pyplot as plt

    # 读取数据
    data = pd.read_csv('seaborn-data/gapminderData.csv')
    # 更改格式
    data['continent'] = pd.Categorical(data['continent'])

    # 分辨率
    dpi = 96

    filenames = []
    # 每年的数据
    for i in data.year.unique():
        # 关闭交互式绘图
        plt.ioff()

        # 初始化
        fig = plt.figure(figsize=(680 / dpi, 480 / dpi), dpi=dpi)

        # 筛选数据
        subsetData = data[data.year == i]

        # 生成散点气泡图
        plt.scatter(
            x=subsetData['lifeExp'],
            y=subsetData['gdpPercap'],
            s=subsetData['pop'] / 200000,
            c=subsetData['continent'].cat.codes,
            cmap="Accent", alpha=0.6, edgecolors="white", linewidth=2)

        # 添加相关信息
        plt.yscale('log')
        plt.xlabel("Life Expectancy")
        plt.ylabel("GDP per Capita")
        plt.title("Year: " + str(i))
        plt.ylim(0, 100000)
        plt.xlim(30, 90)

        # 保存
        filename = './images/' + str(i) + '.png'
        filenames.append(filename)
        plt.savefig(fname=filename, dpi=96)
        plt.gca()
        plt.close(fig)

    # 生成GIF动态图表
    with imageio.get_writer('result.gif', mode='I', fps=5) as writer:
        for filename in filenames:
            image = imageio.imread(filename)
            writer.append_data(image)


if __name__ == "__main__":
    # scatter()
    # Figure()
    # links_filtered()
    # ScalarMappable()
    # folium1()
    # folium()
    # dendrogram()
    # annotate()
    # plot()
    # fill_between()
    # legend()
    stackPlot()
