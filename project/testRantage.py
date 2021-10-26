import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

fig = plt.figure(6)   #创建一个窗口
ax=fig.add_subplot(1,1,1)   #添加一个子图
rect1 = plt.Rectangle((0.1,0.2),0.2,0.3,color='r')  #创建一个矩形，参数：(x,y),width,height
circ1 = plt.Circle((0.7,0.2),0.15,color='r',alpha=0.3)  #创建一个椭圆，参数：中心点，半径，默认这个圆形会跟随窗口大小进行长宽压缩
pgon1 = plt.Polygon([[0.45,0.45],[0.65,0.6],[0.2,0.6]])  #创建一个多边形，参数：每个顶点坐标

ax.add_patch(rect1)  #将形状添加到子图上
ax.add_patch(circ1)  #将形状添加到子图上
ax.add_patch(pgon1)  #将形状添加到子图上

fig.canvas.draw()  #子图绘制
plt.show()