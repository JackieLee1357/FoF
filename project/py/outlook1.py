from email import encoders
from email.header import Header
from email.mime.text import MIMEText

import smtplib


receivers = ['chang_zhang@jabil.com','tenshi_zhang@jabil.com']
mail_content = '天才小熊猫'
mail_title = '天才小熊猫'
smtpServer=r'CORIMC04'
commonPort=587
smtp=smtplib.SMTP("{}:{}".format(smtpServer,commonPort))

# smtp.ehlo()
# smtp.starttls()
# smtp.login(sender, sendermm)
msg = MIMEText(mail_content, "html", 'utf-8')
msg["Subject"] = Header(mail_title, 'utf-8').encode()
msg["to"] = ','.join(receivers)
msg['from']="123@jabil.com"
print(','.join(receivers))
# smtp.sendmail(from_addr="123@jabil.com",to_addrs=','.join(receivers), msg=msg.as_string())


# MAIL_FROM.append('chang_zhang@jabil.com', 'ascii')
#
# MAIL_TO = Header('tenshi_zhang@jabil.com','utf-8')
#
# mm='Jabil@123456789'
#
# msg = MIMEText('123', _charset='utf-8')
# msg['Subject'] = Header('435', 'utf-8')
# msg["From"] = MAIL_FROM
# msg['To'] =MAIL_TO
# smtpServer=r'smtp-mail.outlook.com'
# commonPort=587
# smtp=smtplib.SMTP("{}:{}".format(smtpServer,commonPort))
#
# smtp.ehlo()  # 向邮箱发送SMTP 'ehlo' 命令
# smtp.starttls()
# # smtp.connect(['chang_zhang@jabil.com'])
# smtp.login(str(MAIL_FROM),mm )  # 用户名和密码
#
# smtp.sendmail(str(MAIL_FROM), MAIL_TO, msg.as_string())





