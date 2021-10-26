import requests
from lxml import etree
from fake_useragent import UserAgent
from selenium import webdriver
import os
import io
from pyquery import PyQuery as pq
import time


class getMsgFromJwx:
    def __init__(self):
        self.jwxUrl = 'http://17.80.194.7/#/'
        self.ua = UserAgent()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
            'Cookie': '_cr_ea7075f979add62f2ae4b49cf3e9f2f343bcb64e22ba569cc58d161759c2630c_did=c0fe78c4-e3c8-4346-a6ab-f9d4c84fea9b; _cr_ea7075f979add62f2ae4b49cf3e9f2f343bcb64e22ba569cc58d161759c2630c_skey=73ff24a6-381a-4646-bf5c-f1f992a05ece,1596075413455; _cr_ea7075f979add62f2ae4b49cf3e9f2f343bcb64e22ba569cc58d161759c2630c_allow_events=true',
            "referer": "https://17.80.194.7/#/",
            'Host': '17.80.194.7',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'If-None-Match': '5ed97ccc-270',
            'Upgrade-Insecure-Requests': '1', 'If-None-Match': '5ed97ccc-270',
            }

    def getData(self):
        dataList = []
        url = self.jwxUrl
        print(url)
        response = requests.get(url, headers=self.headers, timeout=5)
        time.sleep(3)
        response.encoding = 'UTF-8'
        print(response.text)
        html = etree.HTML(response.text)
        dataList.append(html)
        return dataList


if __name__ == "__main__":
    getMsg = getMsgFromJwx()
    dataList = getMsg.getData()
    print(dataList)
