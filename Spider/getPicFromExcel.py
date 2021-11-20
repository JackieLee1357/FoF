#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: getPictureFromExcel.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 31, 2021
# ---
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from PIL import ImageGrab


def getPictureFromExcel(sourcePath1, imagePath1):
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = False
    workbook = excel.Workbooks.Open(sourcePath1)
    workSheet = workbook.Worksheets("生产异常汇总")

    for i, shape in enumerate(workSheet.Shapes):
        if shape.Name.startswith('Picture'):
            shape.Copy()
            image = ImageGrab.grabclipboard()
            image.convert('RGB').save(imagePath1)
    workbook.Save()
    workbook.Close()
    excel.Quit()
    print("图片读取成功~")
def sentOutlookMail(dailyReport, people_email):
    mail_content = f'{mailTime}Trace漏失报告'
    smtpServer = r'CORIMC04'
    commonPort = 587
    smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
    message = MIMEMultipart()
    msg = MIMEText(dailyReport, "html", 'utf-8')
    msg["Subject"] = Header(mail_content, 'utf-8').encode()
    from_addr = "OE-FoF_supportTeam@jabil.com"
    receivers = people_email
    receivers = list(receivers)
    msg["To"] = ",".join(receivers)
    msg["from"] = from_addr
    smtp.sendmail(from_addr=from_addr, to_addrs=receivers, msg=msg.as_string())
    smtp.quit()
    print('邮件发送成功')
    print('------------')
