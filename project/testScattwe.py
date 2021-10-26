import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

fig = plt.figure(4)  # 添加一个窗口
ax = fig.add_subplot(1, 1, 1)  # 在窗口上添加一个子图
x = np.random.random(100)  # 产生随机数组
y = np.random.random(100)  # 产生随机数组
ax.scatter(x, y, s=x * 1000, c='y', marker=(5, 1), alpha=0.5, lw=2,
           facecolors='none')  # x横坐标，y纵坐标，s图像大小，c颜色，marker图片，lw图像边框宽度
plt.show()  # 所有窗口运行
