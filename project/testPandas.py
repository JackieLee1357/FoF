from matplotlib import pyplot as plt
import pandas as pd

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

df = pd.read_excel('Metal_20200728在职清册.xlsx')
print(df.head())
departmen_person = pd.pivot_table(df, index=['一级单位'], values=['姓名'], aggfunc='count')
departmen_person.plot.line()
departmen_person.plot.bar()
plt.show()

gender_person = pd.pivot_table(df, index=['性别'], values=['姓名'], aggfunc='count')
gender_person.plot.pie(subplots=True)
gender_person.plot.barh()

plt.show()
