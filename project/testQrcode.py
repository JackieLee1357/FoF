#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testQrcode.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 10, 2021
# ---

import qrcode
import PIL.Image as Image

def makeQrcode(data, path):
    data = '1485928'
    img_file = r'D:\picture\QRcode\qrcode/2.jpg'

    # 实例化QRCode生成qr对象
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=100,
        border=1
    )
    # 传入数据
    qr.add_data(data)

    qr.make(fit=True)

    # 生成二维码
    img = qr.make_image()

    # 保存二维码
    img.save(img_file)
    # 展示二维码
    img.show()

    changeSize(img, img_file)


def changeSize(infile, outfile):
    im = Image.open(r'D:\picture\QRcode\qrcode/2.jpg')
    (x, y) = im.size #read image size
    x_s = 25 #define standard width
    y_s = y * x_s / x #calc height based on standard width
    out = im.resize((x_s, int(y_s)), Image.ANTIALIAS) #resize image with high-quality
    out.save(outfile)
    print('original size: ',x,y)
    print('adjust size: ',x_s,y_s)

    '''
    OUTPUT:
    original size:  500 358
    adjust size:  250 179
    '''

if __name__ == '__main__':
    data = '1485928'
    path = r'D:\picture\QRcode\qrcode/2.jpg'
    makeQrcode(data, path)