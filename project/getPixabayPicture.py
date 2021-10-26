import fake_useragent
import requests
from bs4 import BeautifulSoup
from lxml import etree
import os
from fake_useragent import UserAgent


class Ao3:
    def __init__(self, url):
        self.url = url
        self.ua = fake_useragent.UserAgent(path='C:/Users/Administrator/AppData/Local/Temp/fake_useragent.json')
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": self.ua.random,
            "referer": "https://www.zhihu.com/",
            'Cookie': '''csrftoken=hShe0qp1roKk99ZUqYM6ZFF6MrSxdXMomNBBIGlStmzTImDwkEXXcM7vzy5LzlcB;lang=zh;anonymous_user_id=0645ff6f-b811-4074-ad32-52ee3b9ddfc8; is_human=1;dwf_photo_stats_async=True;__cf_bm=5449601cca116def8c55d045bc4ffa7b30aac373-1627722049-1800-AaCpAjDjUQ/yXXR6T5PXOTspO57EVHqBBTxRgYNGazQnki8PQHHJ2UCIwTjFDSkMR6NlVNip8trzXuIwfvPcM4A=;_ga=GA1.2.355477255.1627722050; _gid=GA1.2.765002367.1627722050; client_width=593''',
        }

    # 获取照片地址列表
    def get_pic_list(self):
        picturl_path_list = []
        url = self.url
        # 访问主网页
        res = requests.get(url, headers=self.headers)
        res.encoding = res.apparent_encoding
        print('response----------')
        print(res.text)
        html = etree.HTML(res.text)
        print("html---------------------------------------")
        print(html)
        urllist = html.xpath('//*[@id="media_container"]/picture/img/@srcset')  # '//*[@id="js_article"]/div/div/div/div/div/section[6]/img/@src'
        print("xpath---------------------------------------")
        print(urllist)
        print('地址解析完成')
        print('----------')
        return urllist

    # 根据图片url下载图片到本地
    def fetch_img(self, path, data_list):
        if not os.path.exists(path):
            os.mkdir(path)
        x = 1
        m = 1
        for pic in data_list:
            picPath = path + str(100 * m + x) + "." + pic[-4:]
            ir = requests.get(pic)
            while os.path.exists(picPath):
                m += 1
                picPath = path + str(100 * m + x) + pic[-4:]
            # 下载图片到本地
            open(picPath, 'wb').write(ir.content)
            print('下载图片%d中...' % (100 * m + x))
            x += 1
        print('图片下载完成')
        print('----------')


if __name__ == '__main__':
    try:
        url = "https://pixabay.com/zh/photos/mountains-sun-clouds-peak-summit-190055/"
        path = "D:\picture\pixabay/"
        if not os.path.exists(path):
            os.mkdir(path)
        print("=====================================")
        print("数据源网址是：" + url)
        print("存储路径是：" + path)
        print("=====================================")
        ao = Ao3(url)
        picturl_path_list = ao.get_pic_list()
        picturl_path_list = list(set(picturl_path_list))
        ao.fetch_img(path, picturl_path_list)
    except Exception as e:
        print(str(e))

