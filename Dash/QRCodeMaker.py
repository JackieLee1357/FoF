#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: test.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site:
# @Time: 8月 10, 2021
# ---
# -*- coding: cp936 -*-
import os
import sys
import qrcode
from PIL import Image


def makeQrcode(data2, img_file):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=100,   # 像素
        border=1
    )  # 实例化QRCode生成qr对象
    qr.add_data(data2)  # 传入数据
    qr.make(fit=True)
    img = qr.make_image()  # 生成二维码
    img.save(img_file)  # 保存二维码
    # img.show()                 # 展示二维码
    changeSize(img, img_file)    # 调整二维码大小


def changeSize(im, outfile):
    (x, y) = im.size  # read image size
    x_s = 43  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = im.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(outfile)


if __name__ == '__main__':
    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path = path0 + "/QRcode.png"
    data = 'Wenjun Xie-986847'
    makeQrcode(data, path)

