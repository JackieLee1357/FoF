#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testHtml.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 18, 2021
# ---
from email.mime.text import MIMEText
import smtplib
import sys
from importlib import reload
import base64
reload(sys)
#sys.setdefaultencoding('utf8')


with open("E:\OneDrive\OneDrive - Jabil\ActionTracker/Trace漏失报表.png", "rb") as f:
    base64_data = base64.b64encode(f.read())
    s = base64_data.decode()

head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>
                table
            {border-collapse:collapse;}
            table, td,tr,th
            {border:1px solid black;padding: 15px}
            </STYLE>
        </head>
        """
body = \
        """
       <body>
        <div align="left",font-family='SimSun',"font-size"="15px">
           <p>Dear Sirs:</p>
           <br>
           <p>以下为Trace每日漏失报告,请查阅！</p>
           <p>发送日期：</p>
           <p></p>
           <p><img src={s}></p>
           <br>
           <a href="https://apps.powerapps.com/play/220d9b3e-6604-4257-9eb3-79e06c05f5b2?tenantId=bc876b21-f134-4c12-a265
           -8ed26b7f0f3b&source=portal&screenColor=rgba(0%2C%20176%2C%20240%2C%201)"target="_blank">请点击此链接登录Power APPs或者用手机登录Power APPs,回复FA/CA！</a>
           <br>
           <br>
           <br>
           <a href="http://cnwgpm0pbi01/Reports/powerbi/WX_MetalSME/official/TEST/Trace%E7%94%9F%E4%BA%A7%E6%BC%8F%E5%A4%B1"
           target="_blank">更多Trace漏失数据请查阅PowerBi，请用厂内网络登录！</a>
           <br>
           <br>
           <p>Best Regards!</p>
           <p>Send by: OE-FoF</p>
           <p>自动邮件,请勿答复</p>
        </div>
    </body>
    """.format(s=s)[0:10]
mailText = "<html>" + head + body + "</html>"
# dataFrame = dataFrame.replace('&&&', '<br>')
print('mail格式处理完毕')
print('------------')
print(mailText)
msg = MIMEText()
msg.attach(mailText)
mailTitle = 'Trace每日漏失报告'
from_addr = "OE-FoF_supportTeam@jabil.com"
receivers = "yuan_li5928@jabil.com"
receivers = receivers.split(",")
smtpServer = r'CORIMC04'
commonPort = 587
print(receivers)
smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
smtp.sendmail(from_addr=from_addr, to_addrs=receivers, msg=msg)
print("邮件发送成功~")
