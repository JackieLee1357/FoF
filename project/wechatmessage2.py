"""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49

微信定时推送消息（非网页版微信登陆的方式）
"""
import json,datetime
import requests,sxtwl,itchat
from wxpy import TEXT
import time


class WechatMessage:
    def __init__(self):
        self.name = ""

    #获得对应的农历
    def getYMD(self):
        ymc = [u"十一", u"十二", u"正", u"二", u"三", u"四", u"五", u"六", u"七", u"八", u"九", u"十"]
        rmc = [u"初一", u"初二", u"初三", u"初四", u"初五", u"初六", u"初七", u"初八", u"初九", u"初十",
               u"十一", u"十二", u"十三", u"十四", u"十五", u"十六", u"十七", u"十八", u"十九",
               u"二十", u"廿一", u"廿二", u"廿三", u"廿四", u"廿五", u"廿六", u"廿七", u"廿八", u"廿九", u"三十", u"卅一"]
        Gan = ["甲", "乙", "丙", "丁", "戊", "己", "庚", "辛", "壬", "癸"]
        Zhi = ["子", "丑", "寅", "卯", "辰", "巳", "午", "未", "申", "酉", "戌", "亥"]
        ShX = ["鼠", "牛", "虎", "兔", "龙", "蛇", "马", "羊", "猴", "鸡", "狗", "猪"]
        numCn = ["天", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"]
        lunar = sxtwl.Lunar()
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        rday = datetime.datetime.now().day
        day = lunar.getDayBySolar(year, month, rday)
        d = str(day.y) + "年" + str(day.m) + "月" + str(day.d) + "日"
        if day.Lleap:
            a = "润" + ymc[day.Lmc] + "月" + rmc[day.Ldi] + "日"
        else:
            a = ymc[day.Lmc] + "月" + rmc[day.Ldi] + "日"
        b = "星期" + numCn[day.week]
        c = Gan[day.Lyear2.tg] + Zhi[day.Lyear2.dz] + "年" + Gan[day.Lmonth2.tg] + Zhi[day.Lmonth2.dz] + "月" + Gan[
            day.Lday2.tg] + Zhi[day.Lday2.dz] + "日"
        txt = '今天日期：'+d + ', ' + b + '\n'+'中华农历: ' + a + ', ' + c
        return txt  #  返回当前的日期信息

    # 爬取爱词霸
    def get_iciba_everyday_chicken_soup(self):
        # 爱词霸的api地址
        url = 'http://open.iciba.com/dsapi/'
        r = requests.get(url)
        all = json.loads(r.text)
        Englis = all['content']
        Chinese = all['note']
        everyday_soup = Chinese+'\n'+Englis+'\n'
        # 返回爱词霸的每日一句
        return everyday_soup

    # 获取天气
    def get_sentence(self, number):
        url = "http://t.weather.sojson.com/api/weather/city/"+ number
        #  向get_sentence 传入参数
        santence = requests.get(url)
        return santence.json()

    # 发送消息
    def send_message(self,message,name):
        url = "https://openai.weixin.qq.com/openapi/sign/"
        print(message,name)





if __name__ == '__main__':
    wm = WechatMessage()
    weather = wm.get_sentence("101190201")
    print(weather)


