#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testPyzbar.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 9月 01, 2021
# ---

import os
import cv2
import requests
from io import BytesIO
from pyzbar import pyzbar
from PIL import Image, ImageEnhance
import pytesseract
import qrcode


def saveImage(frame, filePath):
    # cv2.imshow("layer_1", frame)  # 展示图片
    # cv2.waitKey(0)  # 等待键盘输入
    cv2.destroyAllWindows()  # 销毁所有窗口
    # cv2.imwrite(filePath, frame, [int(cv2.IMWRITE_PNG_COMPRESSION), 9])  # 保存图像
    cv2.imencode('.jpg', frame)[1].tofile(filePath)  # 保存图像,英文或中文路径均适用
    print(f"图片保存完毕{filePath}")
    # cv2.imshow("layer_1", frame)  # 展示图片
    # cv2.waitKey(0)  # 等待键盘输入
    # time.sleep(0.1)


def makeQrcode(data2, img_file):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=200,  # 像素
        border=1
    )  # 实例化QRCode生成qr对象
    qr.add_data(data2)  # 传入数据
    qr.make(fit=True)
    img = qr.make_image()  # 生成二维码
    img.save(img_file)  # 保存二维码
    # img.show()                 # 展示二维码
    changeSize(img_file, 45)  # 调整二维码大小


def changeSize(outfile, width):
    im = Image.open(outfile)
    (x, y) = im.size  # read image size
    x_s = width  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = im.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(outfile)


def imagePaste(pathA, pathB, coordinate2):
    imageA = Image.open(pathA)
    imageB = Image.open(pathB)
    imageA.paste(imageB, coordinate2)  # 将b贴到a的坐标为coordinate的位置
    # imageA.show()                   # 显示a
    # print("二维码贴入图片完毕")
    imageA.save(pathA)


def get_ewm(img_adds):
    """ 读取二维码的内容： img_adds：二维码地址（可以是网址也可是本地地址 """
    if os.path.isfile(img_adds):
        # 从本地加载二维码图片
        img = Image.open(img_adds)
    else:
        # 从网络下载并加载二维码图片
        rq_img = requests.get(img_adds).content
        img = Image.open(BytesIO(rq_img))

    img = ImageEnhance.Brightness(img).enhance(1.65)  # 增加亮度
    # img = ImageEnhance.Sharpness(img).enhance(17.0)#锐利化
    img = ImageEnhance.Contrast(img).enhance(4.2)  # 增加对比度
    img = img.convert('L')  # 灰度化
    # img.show()
    changeSize(image0, img.width + 200)
    txt_list = pyzbar.decode(img)
    dataDict: dict = {}
    print(txt_list)
    for txt in txt_list:
        barcodeData = txt.data.decode("utf-8")
        polygon = txt.polygon[0][0:]
        polygon = tuple(polygon)
        picFile = "pictures/barcode/" + barcodeData + ".png"
        # print(barcodeData)
        # print(polygon)
        dataDict[polygon] = barcodeData
        makeQrcode(barcodeData, picFile)
        imagePaste(image0, picFile, polygon)
    print(f"共识别{len(txt_list)}张二维码~")
    return dataDict


def getImageVar(imgPath):                     # 获取图片的清晰度数据   标准：11921
    imagePath1 = cv2.imread(imgPath)
    img2gray = cv2.cvtColor(imagePath1, cv2.COLOR_BGR2GRAY)
    imageVar = cv2.Laplacian(img2gray, cv2.CV_64F).var()
    cv2.imshow("layer_1", img2gray)  # 展示图片
    cv2.waitKey(0)  # 等待键盘输入
    return int(imageVar)


def getWsNumber(dict):
    # width = 1976
    # height = 960
    b: dict = {}
    for key, value in dict.items():
        i = round(key[0] / width * 5)
        j = round(key[1] / height * 7 + 0.2)
        wsNumber = 7 * (i - 1) + j
        b[wsNumber] = value
    return b


if __name__ == '__main__':
    imageSource = "source/backgroud.jpg"
    image0 = "pictures/backgroud.jpg"  #
    imageSource1 = "source/p2.png"  # 读取图片路径
    print(getImageVar(imageSource1))
    image1 = "pictures/pic1.png"
    image = Image.open(imageSource)  # 打开图片
    image.save(image0)  # 保存图片
    image = Image.open(imageSource1)  # 打开图片
    width = image.width
    height = image.height
    image.save(image1)  # 保存图片
    # picWidth = 1960
    # changeSize(image0, picWidth)
    # changeSize(image1, picWidth)
    # im = Image.open(image0file)      #  识别文字
    # text = pytesseract.image_to_string(im)
    # print(text)

    print("图片处理完毕~")
    result = get_ewm(image1)
    print("二维码读取完毕~")
    result = getWsNumber(result)
    print(result)
    print(range(35))
    for i in range(35):
        i += 1
        try:
            print(f"工位{i}的人员工号为{result[i]}")
        except:
            print(f"工位{i}未获取人员信息~")
            continue
