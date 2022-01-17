#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: test.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 10月 14, 2021
# ---
# coding=utf8


# a = """
# ID,EMT,MachineName,RecipeNumber,ChamberNumber,BiasArcSpeedAlarm,IOCloseDoor,IOInFurnace,IOCoatingEnd,PolyColdTemp,IODefrost,IOHighValve,BaseVacuumSet,BaseTempSet,RotatingSpeed,BiasVoltage,BiasCurrent,DutyCycle,Argon,Nitrogen,Nitrogen2,Acetylene1,Acetylene2,Acetylene3,MF1Voltage,MF1Current,MF2Voltage,MF2Current,MF3Voltage,MF3Current,MF4Voltage,MF4Current,MF5Voltage,MF5Current,MF6Voltage,MF6Current,FurnaceTemp1,FurnaceTemp2,VCGCH1Value,VCGCH2Value,ZDFPV,HP1Voltage1,HP1Current1,HP1Power1,HP1Pulse1,HP1Frequency1,HP1Voltage2
# ,HP1Current2,HP1Power2,HP1Pulse2,HP2Voltage1,HP2Current1,HP2Power1,HP2Pulse1,HP2Frequency1,HP2Voltage2,HP2Current2,HP2Power2,HP2Pulse2,EventTime,CreateTime
# """
# b = a.split("'")
# print(b)
# import datetime
#
# t = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=-2)
# print(t)
import os

path = "//CNWGPM0PG81\Test\Log/"
files = os.listdir(path)
for file in files:
    filePath = path + file
    print(filePath)