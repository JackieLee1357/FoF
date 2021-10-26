import sqlalchemy
import pandas
import numpy as np
import plotly.graph_objs as go
import dash
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调


def conPGSQL(sqlData):  # 链接PG数据库，查询已处理的时段
    db_url = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
    engine = sqlalchemy.create_engine(db_url)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute(sqlData)
    row = cursor.fetchall()
    # row =  [''.join(i) for i in row]    #元组转化为列表
    connection.commit()
    cursor.close()
    engine.dispose()
    df = pandas.DataFrame(row)
    df.columns = [
        '制程',
        '站别',
        '被卡控站点',
        '漏失目标',
        'dri',
        '呈现工站',
        '总数',
        '漏失数量',
        '漏失率',
        '问题点描述',
        '问题类型',
        '日期']
    print('------------')
    return df


def create_Graph(df):
    print(df[df['站别'] == "Band Barcode QC"]['站别']),
    N = 100
    random_x = np.linspace(0, 1, N)
    random_y0 = np.random.randn(N) + 5
    random_y1 = np.random.randn(N)
    random_y2 = np.random.randn(N) - 5
    graph = dcc.Graph(
        id='life-exp-vs-gdp',
        figure=dict(
            data=[
                go.Scatter(
                    x=df[df['站别'] == i]['漏失数量'],
                    y=df[df['站别'] == i]['漏失率'],
                    text=df[df['站别'] == i]['站别'],
                    name=i,
                    mode='markers',
                    opacity=0.8,
                    marker=dict(size=15, line=dict(width=0.5, color='white'))
                ) for i in df.站别.unique()],
            layout=go.Layout(
                xaxis=dict(type='log', title='漏失数量'),
                yaxis=dict(title='漏失率'),
                margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend=dict(x=0, y=1),
                hovermode='closest'
            )
        )
    )
    return graph


def create_table(df, max_rows=12):
    """基于dataframe，设置表格格式"""

    table = html.Table(
        # Header
        [
            html.Tr(
                [
                    html.Th(col) for col in df.columns
                ]
            )
        ] +
        # Body
        [
            html.Tr(
                [
                    html.Td(
                        df.iloc[i][col]
                    ) for col in df.columns
                ]
            ) for i in range(min(10, max_rows))
        ]
    )
    return table


def showDash(data):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    app.layout = html.Div(
        children=[
            html.H4(children='美国农业出口数据表(2011年)'),
            create_table(data),
            create_Graph(data)
        ]
    )
    app.run_server(debug=True)


if __name__ == '__main__':
    querySql = 'select * from actiontracker_data;'
    dataSource = conPGSQL(querySql)
    showDash(dataSource)
