import smtplib
from email.mime.text import MIMEText
from email.header import Header
import string

password = '********'  #这是你邮箱的第三方授权客户端密码，并非你的登录密码
to_addr = '*******@qq.com,*******@163.com'
to_addrs = to_addr.split(',')
print(to_addrs)

