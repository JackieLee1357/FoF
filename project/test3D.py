import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

fig = plt.figure(5)
ax=fig.add_subplot(1,1,1,projection='3d')     #绘制三维图

x,y=np.mgrid[-2:2:20j,-2:2:20j]  #获取x轴数据，y轴数据
z=x*np.exp(-x**2-y**2)   #获取z轴数据

ax.plot_surface(x,y,z,rstride=2,cstride=1,cmap=plt.cm.coolwarm,alpha=0.8)  #绘制三维图表面
ax.set_xlabel('x-name')     #x轴名称
ax.set_ylabel('y-name')     #y轴名称
ax.set_zlabel('z-name')     #z轴名称

plt.show()