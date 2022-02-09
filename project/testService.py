#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: testService.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 14, 2021
# ---
from logging.handlers import TimedRotatingFileHandler
import win32serviceutil
import win32service
import win32event
import os
import logging
import inspect
import time
import shutil


class PythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = "testService"  # 服务名
    _svc_display_name_ = "testService"  # job在windows services上显示的名字
    _svc_description_ = "add number"  # job的描述

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.logger = self._getLogger()
        self.path = 'E:\PythonProjects\OEE\log'
        self.T = time.time()
        self.run = True

    def _getLogger(self):
        """日志记录"""
        logger = logging.getLogger('[PythonService]')
        this_file = inspect.getfile(inspect.currentframe())
        dirPath = os.path.abspath(os.path.dirname(this_file))
        if os.path.isdir('%s\\log' % dirPath):  # 创建log文件夹
            pass
        else:
            os.mkdir('%s\\log' % dirPath)
        path = '%s\\log' % dirPath

        handler = TimedRotatingFileHandler(os.path.join(path, "testService.log"), when="midnight", interval=1,
                                           backupCount=20)
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        return logger

    def SvcDoRun(self):
        self.logger.info("service is run....")
        try:
            i = 0
            while self.run:
                i += 1
                self.logger.info('---Begin---')

                # 服务运行内容
                file = self.path+'/test.txt'
                with open(file, 'w') as fi:
                    fi.write(str(i)+'/n')
                    self.logger.info('已写入'+str(i))
                    fi.close()

                self.logger.info('---End---')
                time.sleep(10)

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
# python testService.py install
# 
# #开启服务
# python testService.py start
# 
# #停止服务
# python testService.py stop
# 
# #移除服务
# python testService.py remove
