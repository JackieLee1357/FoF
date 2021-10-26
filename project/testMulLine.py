import numpy as np
import matplotlib.pyplot as plt
import xlrd
import matplotlib

zhfont1 = matplotlib.font_manager.FontProperties(fname="D:\Python\Tools\SourceHanSansSC-Bold.otf")
file = r'PDCA故障率.xlsx'
data = xlrd.open_workbook(file)
sheet = data.sheet_names()
table = data.sheet_by_name('Sheet2')
colNum = table.ncols
list = []
for i in range(colNum):
    list.append(table.col_values(i))

print(list)
t1 = list[colNum-1][1:]
print(t1)
t = np.arange(0, 2.4, 0.2)
print(t)
x1=t
y1=t1
linstyle = "1"
linewidth = 2
color = "r"
marker = 2
label = "4"
x2=x1
y2=t**2
x3=x1
y3=t**3
plt.title('PDCA故障率',fontproperties = zhfont1)
plt.xlabel('楼栋',fontproperties = zhfont1)
plt.ylabel('故障数量',fontproperties = zhfont1)
linelist=plt.plot(x1,y1)
plt.setp(linelist,color='r')
plt.show()


# 计算正弦和余弦曲线上的点的 x 和 y 坐标
x = np.arange(0,  3  * np.pi,  0.1)
y_sin = np.sin(x)
y_cos = np.cos(x)
# 建立 subplot 网格，高为 2，宽为 1
# 激活第一个 subplot
plt.subplot(2,  1,  1)
# 绘制第一个图像
plt.plot(x, y_sin)
plt.title('Sine')
# 将第二个 subplot 激活，并绘制第二个图像
plt.subplot(2,  1,  2)
plt.plot(x, y_cos)
plt.title('Cosine')
# 展示图像
plt.show()
plt.show()
# 计算正弦和余弦曲线上的点的 x 和 y 坐标
x = np.arange(0,  3  * np.pi,  0.1)
y_sin = np.sin(x)
y_cos = np.cos(x)
# 建立 subplot 网格，高为 2，宽为 1
# 激活第一个 subplot
plt.subplot(2,  1,  1)
# 绘制第一个图像
plt.plot(x, y_sin)
plt.title('Sine')
# 将第二个 subplot 激活，并绘制第二个图像
plt.subplot(2,  1,  2)
plt.plot(x, y_cos)
plt.title('Cosine')
# 展示图像
plt.show()