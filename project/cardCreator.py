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
import configparser
import datetime
import os
import sys
import cv2
import qrcode
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont


def readExcel(pathName):
    # 读取数据源excel，进行数据处理，并返回DataFrame
    df = pd.read_excel(pathName, sheet_name="要求")  # 读取excel
    pd.set_option('display.max_columns', None)  # 显示所有列
    dfList = ["工号", "姓名", "线别", "工站", "栋别+制程", "职称", "主管", "岗位性质"]
    df = df.loc[0:, dfList]
    # print(list(df))  # 显示所有sheet名称
    return df


def cv2ImgAddText(img, text, left, top, textColor2, textSize):
    if isinstance(img, np.ndarray):  # 判断是否OpenCV图片类型
        img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
    draw = ImageDraw.Draw(img)
    fontText = ImageFont.truetype(f"font/{textFont}", textSize, encoding="gb2312")  # 修改字体格式
    draw.text((left, top), text, textColor2, font=fontText)
    return cv2.cvtColor(np.asarray(img), cv2.COLOR_RGB2BGR)


def writeTextToImage(img, data1, coordinate1, textColor1, size1):
    frame = cv2.resize(src=img, dsize=cardSize, interpolation=cv2.INTER_CUBIC)  # 图片尺寸
    x = coordinate1[0]  # 字放置x向位置
    y = coordinate1[1]  # 字放置y位置
    frame = cv2ImgAddText(img=frame, text=data1, left=x, top=y, textColor2=textColor1, textSize=size1)  # 添加文字到图片
    print("文字写入图片完毕")
    return frame


def saveImage(frame, filePath):
    # cv2.imshow("layer_1", frame)  # 展示图片
    # cv2.waitKey(0)  # 等待键盘输入
    cv2.destroyAllWindows()  # 销毁所有窗口
    #cv2.imwrite(filePath, frame, [int(cv2.IMWRITE_PNG_COMPRESSION), 9])  # 保存图像
    cv2.imencode('.jpg', frame)[1].tofile(filePath)  # 保存图像,英文或中文路径均适用
    print(f"图片保存完毕{filePath}")
    # cv2.imshow("layer_1", frame)  # 展示图片
    # cv2.waitKey(0)  # 等待键盘输入
    # time.sleep(0.1)


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


def imagePaste(pathA, pathB, coordinate2):
    imageA = Image.open(pathA)
    imageB = Image.open(pathB)
    imageA.paste(imageB, coordinate2)  # 将b贴到a的坐标为coordinate的位置
    # imageA.show()                   # 显示a
    print("二维码贴入图片完毕")
    imageA.save(pathA)


if __name__ == '__main__':
    time1 = datetime.datetime.now()
    path0 = os.path.abspath(os.path.dirname(sys.argv[0]))  # 获取当前执行文件夹路径
    path0 = path0 + "/cardCreator.ini"
    config = configparser.ConfigParser()
    config.read(path0, encoding="utf-8-sig")  # 导出配置文件
    employeeListSource = config.get("messages", "employeeListSource")  #
    dataSourcePath = config.get("messages", "dataSourcePath")  #
    QRcodePath = config.get("messages", "QRcodePath")  #
    IDCardPath = config.get("messages", "IDCardPath")  #
    textFont = config.get("messages", "textFont")  #
    cardSize = (300, 120)     # 调整胸卡尺寸大小
    employeeList = readExcel(employeeListSource)
    for i in range(len(employeeList)):
        organizationName = employeeList.loc[i, "栋别+制程"]
        employeeName = employeeList.loc[i, "姓名"]
        WDNumber = str(employeeList.loc[i, "工号"])
        department1 = employeeList.loc[i, "工站"]
        department2 = employeeList.loc[i, "线别"]
        jobTitle = employeeList.loc[i, "职称"]
        subject = employeeList.loc[i, "岗位性质"]
        managerName = employeeList.loc[i, "主管"]
        dataList = {
            (0, 68): organizationName.center(21,),
            (0, 93): managerName.center(30,),
            (140, 8): department1.center(15,),
            (140, 29): jobTitle.center(15,),
            (205, 8): department2.center(15,),
            (205, 29): subject.center(15,),
            (135, 63): employeeName.center(15,),
            (135, 89): WDNumber.center(15,)
        }  # 数据源
        pathIndex = employeeName + WDNumber
        print(f"第{i}条数据姓名工号为=================")
        print(pathIndex)
        print(dataList)
        print(subject)
        qrPath = fr'{QRcodePath }{pathIndex}QRCode.jpg'
        makeQrcode(WDNumber, qrPath)  # 生成二维码
        sourcePath = f"{dataSourcePath }{subject}.jpg"
        path = f'{IDCardPath }{pathIndex}Card.jpg'
        image = Image.open(sourcePath)  # 打开图片
        image_resize = image.resize(cardSize)  # 设置图片尺寸
        image_array = np.array(image_resize)
        image_output = Image.fromarray(image_array)
        image_output.save(path)  # 保存图片
        for j in range(len(dataList)):
            print(f"第{j}个参数为=================")
            textColor = (0, 0, 0)  # 颜色
            size = 14  # 字体大小
            if j < 2:
                textColor = (255, 255, 255)
            if j > 5:
                size = 16
            # image = cv2.imread(path)  # 用于读取英文路径下的图片
            image = cv2.imdecode(np.fromfile(path, dtype=np.uint8), 1)   # 读取中文路径的图片
            print(f"读取图片{path}")
            data = list(dataList.values())[j]
            coordinate = list(dataList.keys())[j]
            print(coordinate)
            print(data)
            image = writeTextToImage(image, str(data), coordinate, textColor1=textColor, size1=size)  # 把文字写入图片
            saveImage(image, path)
        imagePaste(path, qrPath, (242, 67))     # 把二维码贴入图片对应位置
    time2 = datetime.datetime.now()
    print(f"已生成{len(employeeList)}条数据，用时{time2-time1}")
