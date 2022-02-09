#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: sendMail.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 12月 01, 2021
# ---

import base64
import os
import smtplib
import sys
import traceback
from email.header import Header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class SendMail(object):
    def __init__(self, content=None,
                 image=None, file=None, sender=None, receiver=None, subject=None, cc=None):
        """
               :param receiver: 收件人，多个要传list ['a@qq.com','b@qq.com]
               :param content: 邮件正文
               :param image: 图片路径，绝对路径，默认为无图片
               :param file: 附件路径，如果不在当前目录下，要写绝对路径，默认没有附件
        """
        self.from_addr = sender
        self.receiver = receiver  # 收件人，多个要传list ['a @ qq.com','b @ qq.com]
        self.cc = cc  # cc人员
        self.subject = subject  # 邮件标题
        self.content = content  # 邮件正文
        self.image = image  # 图片路径（绝对路径）
        self.file = file  # 文件路径（绝对路径）
        self.message = MIMEMultipart()  # 构造一个MIMEMultipart对象代表邮件本身

        # 添加文件到附件
        if self.file:
            file_name = os.path.split(self.file)[-1]  # 只取文件名，不取路径
            try:
                f = open(self.file, 'rb').read()
            except Exception as e:
                traceback.print_exc()
            else:
                att = MIMEText(f, "base64", "utf-8")
                att["Content-Type"] = 'application/octet-stream'
                # base64.b64encode(file_name.encode()).decode()
                new_file_name = '=?utf-8?b?' + base64.b64encode(file_name.encode()).decode() + '?='
                # 处理文件名为中文名的文件
                att["Content-Disposition"] = 'attachment; filename="%s"' % (new_file_name)
                self.message.attach(att)

        # 添加图片到附件
        if self.image:
            try:
                with open(self.image, 'rb') as f:
                    # 将图片显示在邮件正文中
                    msgimage = MIMEImage(f.read())
                    msgimage.add_header('Content-ID', '<image1>')  # 指定文件的Content-ID,<img>,在HTML中图片src将用到
                    self.message.attach(msgimage)
            except Exception as e:
                traceback.print_exc()

    def send_mail(self):
        to_addrs = self.receiver + self.cc
        smtpServer = r'CORIMC04'
        commonPort = 587
        smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
        self.message.attach(MIMEText(self.content, 'html', 'utf-8'))  # 正文内容   plain代表纯文本,html代表支持html文本
        self.message['From'] = self.from_addr  # 发件人
        self.message['To'] = ','.join(self.receiver)  # 收件人
        self.message['Subject'] = Header(self.subject, 'utf-8').encode()  # 邮件标题
        self.message['Cc'] = ','.join(self.cc)  # self.cc
        # 发送邮件服务器的对象
        print('收件人为------------------')
        print(self.receiver)
        print('抄送------------------')
        print(self.cc)
        try:
            smtp.sendmail(from_addr=self.from_addr, to_addrs=to_addrs, msg=self.message.as_string())
            print("邮件发送成功！")
            pass
        except Exception as e:
            resultCode = 0
            traceback.print_exc()
        else:
            resultCode = 1
        smtp.quit()
        return resultCode


if __name__ == '__main__':
    sender = "OE-FoF_SupportTeam@jabil.com"
    receiver = ['yuan_li5928@jabil.com', '', '']  # 收件人，多个要传list ['a @ qq.com','b @ qq.com]
    cc = ['yuan_li5928@jabil.com']  # cc人员
    subject = 'OEE程式运行结果报告'  # 邮件标题
    content = 'It is a test'  # 邮件正文
    image = None  # 图片路径（绝对路径）
    file = None  # 文件路径（绝对路径）
    mail = SendMail(content=content, sender=sender, receiver=receiver, cc=cc, subject=subject, image=image, file=file)
    mail.send_mail()  # 发邮件
    sys.exit(0)
