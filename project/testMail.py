#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testMail.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 19, 2021
# ---
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def sentOutlookMail(dailyReport):
    mail_content = 'Trace每日漏失报告'
    smtpServer = r'CORIMC04'
    commonPort = 587
    smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
    message = MIMEMultipart()
    msg = MIMEText(dailyReport, "html", 'utf-8')
    msg["Subject"] = Header(mail_content, 'utf-8').encode()
    from_addr = "OE-FoF_supportTeam@jabil.com"
    receivers = "xianjie_luo@jabil.com,"
    cc = "yuan_li5928@jabil.com,"
    to_addrs = ["xianjie@jabil.com", "yuan_li5928@jabil.com"]
    msg["To"] = receivers
    msg["from"] = from_addr
    msg["cc"] = cc
    smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=msg.as_string())
    smtp.quit()
    print('邮件发送成功')
    print('------------')


data = 'FYI'
sentOutlookMail(data)
print("-------")