"""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49

微信定时推送消息（网页版微信登陆的方式，图灵机器人）
"""
import json,datetime
import requests,sxtwl,itchat
from wxpy import TEXT
import time


class WechatMessage:
    def __init__(self):
        self.name = "D:/fille/"

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
        url = 'http://open.iciba.com/dsapi/' # 爱词霸的api地址
        r = requests.get(url)
        all = json.loads(r.text)
        Englis = all['content']
        Chinese = all['note']
        everyday_soup = Chinese+'\n'+Englis+'\n'
        return everyday_soup  #  返回爱词霸的每日一句

    # 获取天气
    def get_sentence(self, api):
        santence = requests.get(api)
        return santence.json()


    def get_response(self, question):
        apikey = '415b9691c05c431698e2125d85aa1632'
        url = 'http://openapi.tuling123.com/openapi/api/v2' + apikey + '&info=' + question
        res = requests.get(url).json()
        return res['text']

    #  微信机器人
    @itchat.msg_register(TEXT, isFriendChat=True)
    def auto_reply(self, msg):
        print("消息是：%s" % msg['Content'])
        itchat.send_msg(wm.get_response(msg['Content']), toUserName=msg['FromUserName'])
        print('auto_reply:%s' % wm.get_response(msg['Content']))



if __name__ == '__main__':
    wm = WechatMessage()
    # names  = input("请输入你要发送人的微信名：")
    hours = int(input("请输入几点发送消息："))
    minutes = int(input("请输入几分发送消息："))
    # number = input("输入所在城市的编号：")
    names  = "灰"
    number = "101190201"
    g = wm.getYMD()
    g1 = wm.get_iciba_everyday_chicken_soup()
    #  天气接口的网站 number为城市编号
    name = "http://t.weather.sojson.com/api/weather/city/"+ number
    #  向get_sentence 传入参数
    g2 = wm.get_sentence(name)
    times = g2['cityInfo']
    for key, name in times.items():
        city = times['city']
        parent = times['parent']
    #  字典嵌套字典
    time1 = g2['data']
    for key, name in time1.items():
        shidu = time1['shidu']
        pm25 = time1['pm25']
        quality = time1['quality']
        ganmao = time1['ganmao']
    time1 = g2['data']
    time2 = time1.get('forecast', '不存在该键')
    time2 = time2[0]
    itchat.auto_login(hotReload=True)
    for key, name in time2.items():
        high = time2['high']
        low = time2['low']
        fx = time2['fx']
        fl = time2['fl']
        type = time2['type']
        notice = time2['type']
    #  调用微信机器人
    users = itchat.search_friends(names)  # 找到用户
    userName = users[0]['UserName']

    while True:

        t = datetime.datetime.now()
        t1=t.strftime('%Y-%m-%d %H:%M:%S')
        hour = t.hour
        minute = t.minute
        second = t.second
        print('%d:%d:%d' % (hour,minute,second))
        if hour == hours and minute == minutes:
            itchat.send_msg("%s" % g, toUserName=userName)
            itchat.send_msg('%s' % g1, toUserName=userName)
            itchat.send_msg('所在省份：%s\n'
                            '所在城市：%s\n'
                            '今日最高温度：%s\n '
                            '今日最低温度：%s\n'
                            '风向：%s\n '
                            '风力：%s\n'
                            '湿度：%s \n'
                            'PM2.5: %s\n'
                            '空气质量：%s \n'
                            '易感指数：%s\n'
                            '天气：%s - %s '%(parent,city,high,low,fx,fl,shidu,pm25,
                                           quality,ganmao,type,notice), toUserName=userName)
            break
        else:
            time.sleep(5)  #  延迟5秒
            continue
    itchat.run()
    time.sleep(86400)

