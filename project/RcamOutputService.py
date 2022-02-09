#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: RcamOutputService.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 14, 2021
# ---
import base64
import configparser
import datetime
import inspect
import logging
import os
import smtplib
import time
import traceback
from email.header import Header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import TimedRotatingFileHandler
import pandas
import pymssql
import win32event
import win32service
import win32serviceutil


class RcamOutput(object):
    def __init__(self, people_email, cc_email):
        self.people_email = people_email
        self.cc_email = cc_email

    def run(self):
        dateNow = datetime.datetime.now()  # + datetime.timedelta(minutes=-1)
        if (dateNow.hour >= 7) & (dateNow.hour < 21):
            dateNow = datetime.datetime.now()  # + datetime.timedelta(minutes=-1)
            if '1' == str(dateNow.time().minute)[-1]:  # 判断分钟是否包含1
                mailInfo = self.getMailData(dateNow)
                if mailInfo is not None:
                    m = SendMail(content=mailInfo, people_email=self.people_email, cc_email=self.cc_email)
                    m.send_mail()
                    return '邮件已发送~'

        return None

    def getMailData(self, now):
        tableColumns = ['ID', '制程', '标准产出', '良品数', '抛料数']
        processDic = {'br-assy': 'Brace Assembly', 'washer-assy': 'Washer Assembly',
                      'trim-br-wld': 'Trim Washer to Brace',
                      'br-bd-wld-r': 'Brace to Band Welding Right', 'br-bd-wld-t': 'Brace to Band Welding Top',
                      'bt-wld': 'Bracket Welding'}
        processTup = tuple(processDic.keys())
        sqlData = f"""SELECT stationid,
                            process,
                            (case when process='br-assy' then 17
                                when process='washer-assy' then 17
                                when process='trim-br-wld' then 34
                                when process='br-bd-wld-r' then 17
                                when process='br-bd-wld-t' then 17
                                when process='bt-wld' then 38 end) as Stardard,
                            count(case when event='pass' then 1 end) as passCount,
                            count(case when event='fail' then 1 end) as failCount
                    from V_History_Temp a WITH(NOLOCK)
                    where process in {processTup}
                    and created between dateadd(minute,-15,getdate()) and dateadd(minute,-5,getdate())
                    group by stationid,process
                    order by process, stationid"""
        mailInfoTable = pandas.DataFrame(self.conSqlS(sqlData))  # 从数据库获取信息
        if len(mailInfoTable) == 0:
            print("无数据~")
            return None
        mailInfoTable.columns = tableColumns
        mailInfoTable = mailInfoTable[mailInfoTable['良品数'] < mailInfoTable['标准产出']]
        mailInfoTable['楼栋'] = [i.split('_')[1].split('F-')[0] + 'F' for i in mailInfoTable['ID']]
        mailInfoTable['机台号'] = [i.split('_')[2] for i in mailInfoTable['ID']]
        mailInfoTable = mailInfoTable[['制程', '楼栋', '机台号', '标准产出', '良品数', '抛料数']]
        for key, value in processDic.items():
            mailInfoTable = mailInfoTable.replace(key, value)  # 替换制程名称
        styles = [dict(selector='td', props=[("text-align", "center")]),
                  dict(selector="th", props=[("font-size", "15px"), ("text-align", "center")]),
                  dict(selector="caption", props=[("caption-side", "top")]),
                  dict(selector="tr:hover", props=[("background-color", "white")])]  # hover悬停

        mailInfoTable = mailInfoTable.style.set_table_styles(styles).applymap(self.changeColor,
                                                                              subset=['良品数']).hide_index().render()
        mailInfoTable = self.changeStyle(mailInfoTable, now)
        return mailInfoTable

    @staticmethod
    def conSqlS(sqlData):
        # try:
        conn = pymssql.connect(
            host='CNWXIM0TRSQLV2A',  # 主机名或ip地址
            user='PBIuser',  # 用户名
            password='PBIuser123456',  # 密码
            charset='UTF-8',  # 字符编码
            database='Trace_D63')  # 库名
        cursor = conn.cursor()
        cursor.execute(sqlData)
        row = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        # print('数据库操作完成~')
        return row

    @staticmethod
    def changeStyle(mailInform, dateNow1):
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
               <p>以下为Zurich专案Rcam各站未达标机台产能报告：</p>
               <p>发生时间：{sh_sys}</p>
               <p>{dataFrame}</p>
               <br>
               <p>有任何问题或建议，请联系我们~</p>
               <br>
               <br>
               <p>Best Regards!</p>
               <p>Send by: OE-FoF</p>
               <p>Tel: 8189-5468</p>
               <p>自动邮件,请勿答复</p>
            </div>
        </body>
        """.format(dataFrame=mailInform, sh_sys=str(dateNow1)[0:15] + '0')  # 整10分发送
        mailText = "<html>" + head + body + "</html>"
        # dataFrame = dataFrame.replace('&&&', '<br>')
        # print('mail格式处理完毕')
        # print('------------')
        return mailText

    @staticmethod
    def changeColor(val):
        """
        :param val:元素值，表达式dataframe中的所有值，单个传入 对单个值的字体颜色修改
        :return: val
        """
        try:
            if val < 50:  # 报警规则
                # return 'background-color:yellow'
                return 'color:#BC1717'
            else:
                # return 'background-color:white'
                return 'color:black'
        except Exception as ex:
            print(ex)
            return 'color:black'


class SendMail(object):
    def __init__(self, content=None,
                 image=None, file=None, people_email=None, cc_email=None):
        """
               :param recv: 收件人，多个要传list ['a@qq.com','b@qq.com]
               :param content: 邮件正文
               :param image: 图片路径，绝对路径，默认为无图片
               :param file: 附件路径，如果不在当前目录下，要写绝对路径，默认没有附件
        """

        self.recv = people_email  # 收件人，多个要传list ['a @ qq.com','b @ qq.com]
        self.cc_email = cc_email  # cc人员
        self.title = 'Zurich专案Rcam各站未达标机台产能报告'  # 邮件标题
        self.content = content  # 邮件正文
        self.image = image  # 图片路径（绝对路径）
        self.file = file  # 文件路径（绝对路径）
        self.message = MIMEMultipart()  # 构造一个MIMEMultipart对象代表邮件本身

        # 添加文件到附件
        if self.file:
            file_name = os.path.split(self.file)[-1]  # 只取文件名，不取路径
            try:
                f = open(self.file, 'rb').read()
                f = str(f)
            except:
                traceback.print_exc()
            else:
                att = MIMEText(f, "base64", "utf-8")
                att["Content-Type"] = 'application/octet-stream'
                # base64.b64encode(file_name.encode()).decode()
                new_file_name = '=?utf-8?b?' + base64.b64encode(file_name.encode()).decode() + '?='
                # 处理文件名为中文名的文件
                att["Content-Disposition"] = 'attachment; filename="%s"' % new_file_name
                self.message.attach(att)

        # 添加图片到附件
        if self.image:
            try:
                with open(self.image, 'rb') as f:
                    # 将图片显示在邮件正文中
                    msgimage = MIMEImage(f.read())
                    msgimage.add_header('Content-ID', '<image1>')  # 指定文件的Content-ID,<img>,在HTML中图片src将用到
                    self.message.attach(msgimage)
            except:
                traceback.print_exc()

    def send_mail(self):
        mailTitle = self.title
        from_adds = "OE-FoF_Support_Team@jabil.com"
        receivers = self.recv
        cc = self.cc_email
        to_adds = receivers + cc
        smtpServer = r'CORIMC04'
        commonPort = 587
        smtp = smtplib.SMTP("{}:{}".format(smtpServer, commonPort))
        self.message.attach(MIMEText(self.content, 'html', 'utf-8'))  # 正文内容   plain代表纯文本,html代表支持html文本
        self.message['From'] = from_adds  # 发件人
        self.message['To'] = ','.join(receivers)  # 收件人
        self.message['Subject'] = Header(mailTitle, 'utf-8').encode()  # 邮件标题
        self.message['Cc'] = ','.join(cc)  # cc
        # 发送邮件服务器的对象
        # print('收件人为------------------')
        # print(receivers)
        # print('抄送------------------')
        # print(cc)
        try:
            smtp.sendmail(from_addr=from_adds, to_addrs=to_adds, msg=self.message.as_string())
            # print("邮件发送成功！")
            pass
        except:
            resultCode = 0
            traceback.print_exc()
        else:
            resultCode = 1
        smtp.quit()
        return resultCode


class PythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = "RcamOutputService"  # 服务名
    _svc_display_name_ = "RcamOutputService"  # job在windows services上显示的名字
    _svc_description_ = "RcamOutputService"  # job的描述

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        this_file = inspect.getfile(inspect.currentframe())
        self.path = os.path.abspath(os.path.dirname(this_file))
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.logger = self._getLogger()
        self.run = True

    def _getLogger(self):
        """日志记录"""
        logger = logging.getLogger('[PythonService]')
        this_file = inspect.getfile(inspect.currentframe())
        self.path = os.path.abspath(os.path.dirname(this_file))
        if os.path.isdir('%s\\log' % self.path):  # 创建log文件夹
            pass
        else:
            os.mkdir('%s\\log' % self.path)
        path = '%s\\log' % self.path
        now = datetime.datetime.now().date().strftime("%Y-%m-%d")
        fileName = "RcamOutput%s.log" % now
        fileName1 = fileName  # "RcamOutputService.log"
        handler = TimedRotatingFileHandler(os.path.join(path, fileName1), when="midnight", interval=1,
                                           backupCount=20)
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def SvcDoRun(self):
        self.logger.info("service is run....")
        try:
            while self.run:
                # self.logger.info('---Begin---')
                # 服务运行内容
                iniPath = self.path + "/RcamOutput.ini"
                config = configparser.ConfigParser()
                try:
                    config.read(iniPath, encoding="utf-8-sig")  # 导出+配置文件,解决文档格式为utf-8-bom问题
                except Exception as e:
                    print(e)
                    config.read(iniPath, encoding="utf-8")  # 导出配置文件
                people_email = config.get("messages", "people_email")  # 接收邮件人员
                cc_email = config.get("messages", "cc_email")  # cc邮件人员
                people_email = people_email.split(",")  # 字符串根据","进行分割，转换为list
                cc_email = cc_email.split(",")
                rcam = RcamOutput(people_email=people_email, cc_email=cc_email)
                s = rcam.run()
                if s is not None:
                    self.logger.info(s)
                else:
                    self.logger.info('服务器待命中')
                time.sleep(60)

        except Exception as e:
            self.logger.info(e)
            time.sleep(60)

    def SvcStop(self):
        self.logger.info("service is stop....")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.run = False


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(PythonService)


# #安装服务
# python RcamOutputService.py install
# 
# #开启服务
# python RcamOutputService.py start
# 
# #停止服务
# python RcamOutputService.py stop
# 
# #移除服务
# python RcamOutputService.py remove
