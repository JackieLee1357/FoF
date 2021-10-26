# -*- coding: utf-8 -*- 
# @Time : 2021/4/23 11:04 
# @Author : chang
# @File : outlook.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.image import MIMEImage


multiPart=MIMEMultipart()
fromreal ='chang_zhang@jabil.com'
fromshow = 'pkq@jabil.com'
toaddy = ['tenshi_zhang@jabil.com']
subject='Hello,World!'
multiPart['Subject']=Header(subject,"utf-8")
body = '天才皮卡丘'
password='Jabil@123456789'
msg = MIMEText('hello, send by Python...', 'plain', 'utf-8')
msg['From'] =fromreal
msg['To'] =toaddy
msg['Subject']=Header(u'123', 'utf-8').encode()
server = r'smtp-mail.outlook.com'
port = 587
mail = smtplib.SMTP(server, port)
mail.ehlo()
mail.starttls()
mail.login(fromreal, password)
mail.sendmail(fromreal, toaddy, msg.as_string())
print('E-mail sent.')
mail.close()
# emailBody=MIMEText('123')
# multiPart.attach(emailBody)
# smtpServer=r'smtp-mail.outlook.com'
# commonPort=587
# smtp=smtplib.SMTP("{}:{}".format(smtpServer,commonPort))
# user='chang_zhang@jabil.com'
# emailPwd='Jabil@123456789'
# sender='chang_zhang@jabil.com'
# receiverList='Tenshi_Zhang@jabil.com'
# smtp.ehlo()
# smtp.starttls()
# smtp.login(user,emailPwd)
# smtp.sendmail(fromshow,receiverList,multiPart.as_string())