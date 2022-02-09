#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: createBarcode.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 9月 02, 2021
# ---


import os
import cv2
import requests
from io import BytesIO
from pyzbar import pyzbar
from PIL import Image, ImageEnhance
import qrcode


# def create_ewm(data1, img_file):
#     """ 生成二维码 """
#     qr = qrcode.QRCode(
#         version=1,
#         error_correction=qrcode.constants.ERROR_CORRECT_H,
#         box_size=10,
#         border=4
#     )
#     # 传入数据
#     qr.add_data(data1)
#     qr.make(fit=True)
#     # 生成二维码
#     img = qr.make_image()
#     # 保存二维码
#     img.save(img_file)
#     # 显示二维码
#     # img.show()
#     print(img_file+"制作OK~")


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
    changeSize(img, img_file)  # 调整二维码大小


def changeSize(im, outfile):
    (x, y) = im.size  # read image size
    x_s = 45  # 调整二维码大小
    y_s = y * x_s / x  # calc height based on standard width
    out = im.resize((x_s, int(y_s)), Image.ANTIALIAS)  # resize image with high-quality
    out.save(outfile)


def imagePaste(pathA, pathB, coordinate2):
    imageA = Image.open(pathA)
    imageB = Image.open(pathB)
    imageA.paste(imageB, coordinate2)  # 将b贴到a的坐标为coordinate的位置
    # imageA.show()                   # 显示a
    print("二维码贴入图片完毕")
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

    img = ImageEnhance.Brightness(img).enhance(1.65)#增加亮度

    # img = ImageEnhance.Sharpness(img).enhance(17.0)#锐利化

    img = ImageEnhance.Contrast(img).enhance(4.2)#增加对比度

    img = img.convert('L')  # 灰度化

    img.show()

    txt_list = pyzbar.decode(img)
    print(txt_list)
    for txt in txt_list:
        barcodeData = txt.data.decode("utf-8")
        polygon = txt.polygon[0][0:]
        polygon = tuple(polygon)
        picFile = "pictures/barcode/" + barcodeData + ".png"
        print(barcodeData)
        print(polygon)
        makeQrcode(barcodeData, picFile)
        imagePaste(image0file0, picFile, polygon)

    print(f"共识别{len(txt_list)}张二维码~")


if __name__ == '__main__':
    # 生成二维码
    # datas = range(35)
    # datas2 = range(35)
    # imageFiles = ["pictures/" + str(i + 1000) + ".jpg" for i in datas]
    image0file1 = "backgroud.jpg"
    image0file0 = "pictures/backgroud.jpg"   #
    image0file = "pictures/backgroud2.jpg"  # 读取图片路径
    # imageFiles2 = ["pictures/" + str(i + 1) + ".jpg" for i in datas]
    # print(datas)
    # print(imageFiles)
    #
    pictureSize = (1696, 876)
    image0 = Image.open(image0file1)  # 打开图片
    image1 = image0.resize(pictureSize)  # 设置图片尺寸
    image1.save(image0file0)  # 保存图片
    #
    # for i in range(len(imageFiles)):
    #     data1 = datas[i]
    #     imageFile = imageFiles[i]
    #     coordinate = (int((i % 5) * pictureSize[0] / 5) + 40, int(pictureSize[1] / 5 * int(i / 5)) + 40)
    #     makeQrcode("WS0"+str(data1), imageFile)
    #     data1 = datas2[i]
    #     imageFile2 = imageFiles2[i]
    #     coordinate2 = (int((i % 5) * pictureSize[0] / 5) + 40, int(pictureSize[1] / 5 * int(i / 5)) + 10)
    #     makeQrcode(data1, imageFile2)
    #
    #     # image0 = Image.open(image0file0)
    #     # image0.save(image0file)
    #     image0 = Image.open(image0file)  # 打开图片
    #     image1 = image0.resize(pictureSize)  # 设置图片尺寸
    #     image1.save(image0file)  # 保存图片
    #     print("~~~~~~~~~~~")
    #     print(image0file)
    #     print(imageFile)
    #     print(coordinate)
    #     imagePaste(image0file, imageFile, coordinate)  # 把二维码贴入图片对应位置
    #     imagePaste(image0file, imageFile2, coordinate2)  # 把二维码贴入图片对应位置
    print("图片处理完毕~")
    get_ewm(image0file)
    print("二维码读取完毕~")
    # create_ewm(data1, imageFile)
    # 解析本地二维码
    # get_ewm(imageFile)
    #
    #

    # create_ewm('https://www.baidu.com', 'D:\\code.png')
    # # 解析网络二维码
    # get_ewm('https://gqrcode.alicdn.com/img?type=cs&shop_id=445653319&seller_id=3035998964&w=140&h=140&el=q&v=1')
