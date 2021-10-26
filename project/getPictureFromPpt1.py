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
            "referer": "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_pic_list(self):
        picturl_path_list = []
        url = self.url
        # 访问主网页
        res = requests.get(url, headers=self.headers)
        res.encoding = 'utf-8'
        print('response----------')
        # print(res.text)
        html = etree.HTML(res.text)
        print("html---------------------------------------")
        urllist = html.xpath('/html/body/div[1]/div[2]/div[1]/div/div[1]/div[3]/section/img/@data-src')  # '//*[@id="js_article"]/div/div/div/div/div/section[6]/img/@src'
        print("urllist---------------------------------------")
        print(urllist)
        print('地址解析完成')
        print('----------')
        return urllist

    # 根据图片url下载图片到本地
    def fetch_img(self, path, data_list):
        if not os.path.exists(path):
            os.mkdir(path)
        x = 400
        while x < len(data_list):
            pic = data_list[x]
            picPath = path + str(x) + "." + pic[-4:]
            ir = requests.get(pic)
            while os.path.exists(picPath):
                picPath = path + str(x) + pic[-4:]
            # 下载图片到本地
            open(picPath, 'wb').write(ir.content)
            print('下载图片%d中...' % (x))
            x += 1
        print('图片下载完成')
        print('----------')


if __name__ == '__main__':
    try:
        url = "https://mp.weixin.qq.com/s/cfjYqCzDRMfoCbihSnCcuA"
        path = "D:\picture\weixin/"
        if not os.path.exists(path):
            os.mkdir(path)
        print("=====================================")
        print("数据源网址是：" + url)
        print("存储路径是：" + path)
        print("=========================== ==========")
        ao = Ao3(url)
        picturl_path_list = ao.get_pic_list()
        picturl_path_list = list(picturl_path_list)
        ao.fetch_img(path, picturl_path_list)
    except Exception as e:
        print(str(e))

