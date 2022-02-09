#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: barcodeCreator.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 25, 2021
# ---

import qrcode
import barcode
from barcode.writer import ImageWriter
from PIL import Image


def createQRImage(data, qrImage):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=300,
        border=1,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image()
    (x, y) = img.size  # read image size
    x_s = 150  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = img.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(qrImage)


def createBarImage(data, barcodeImage):
    bar = barcode.get_barcode_class("code128")
    img = bar(data, writer=ImageWriter())
    img.save(barcodeImage, {'dpi': 800, 'module_width': 0.8, 'module_height': 40, 'font_size': 170})
    img = Image.open(barcodeImage)
    (x, y) = img.size  # read image size
    x_s = 350  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = img.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(barcodeImage)


if __name__ == '__main__':
    data = 'WLJC20210825018'
    model = 'C:/transferCard/transferCard.jpg'
    qrImage = 'aqr.png'
    barcodeImage = 'abarcode.png'
    resultImage = 'code.png'
    createQRImage(data, qrImage)
    createBarImage(data, barcodeImage)
    im = Image.open(model)
    (x, y) = im.size  # read image size
    x_s = 600  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = im.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(resultImage)
    image0 = Image.open(resultImage)
    imageA = Image.open(qrImage)
    imageB = Image.open(barcodeImage)
    image0.paste(imageA, (410, 125))  # 将b贴到a的坐标为coordinate的位置
    print("二维码贴入图片完毕")
    image0.save(resultImage)

    image0 = Image.open(resultImage)
    image0.paste(imageB, (15, 125))  # 将b贴到a的坐标为coordinate的位置
    # image0.show()  # 显示a
    print("二维码贴入图片完毕")
    image0.save(resultImage)
