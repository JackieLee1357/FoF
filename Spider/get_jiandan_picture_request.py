""""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49
爬取煎蛋网页上的图片
"""
import requests
from lxml import etree
from fake_useragent import UserAgent
from selenium import webdriver
import os
import io

class Ao3:
    def __init__(self):
        self.jiandan_url = "http://jandan.net/ooxx/MjAyMDA3MjUtMT"
        self.ua = UserAgent()
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": self.ua.random ,
            "referer":  "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_jiandan_list(self):
        picturl_path_list = []
        jiandan_url = self.jiandan_url
        for i in range(10):
            print(i)
            # 访问主网页
            res = requests.get(jiandan_url, headers=self.headers)
            res.encoding='utf-8'
            #print(res.text)
            root = etree.HTML(res.text)
            picturl_path_list = picturl_path_list + root.xpath('//*[@class="commentlist"]/li/div/div/div[2]/p/a/@href')
            #print(picturePathList)
            jiandan_url = "http:" + root.xpath('//*[@id="comments"]/div[2]/div/a[1]/@href')[0]
            #print("下一页："+jiandan_url)
        return picturl_path_list

    # 根据图片url下载图片到本地
    def fetch_img(self, path, data_list):
        if not os.path.exists(path):
            os.mkdir(path)

        x = 0
        for list in data_list:
            list = "http:" + list
            #print(list)
            ir = requests.get(list)
            # 下载图片到本地
            open(path + str(x) + list[-4:], 'wb').write(ir.content)
            x += 1

if __name__ == '__main__':
    ao = Ao3()
    path1 = "D:/picture/tupianzj/"
    path = ''
    if not os.path.exists(path1):
            os.mkdir(path1)
            path = path1
    else:
        for i in range(len(path1)-1):
            path += path1[i]
    path = path + '-new/' 
    picturl_path_list = ao.get_jiandan_list()
    picturl_path_list = list(set(picturl_path_list))
    print("=====================================")
    print(picturl_path_list)
    print("=====================================")
    ao.fetch_img(path,picturl_path_list)

