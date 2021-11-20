import requests
import random
import os
import re

class Ao3:
    def __init__(self, url):
        self.url = url
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87',
            "referer": "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_pic_list(self):
        picture_url_list = []
        url1 = self.url
        # 访问主网页
        html = requests.get(url1, headers=self.headers).text
        url_list = re.findall('/se5y-tttppp/+\d+.html',html,re.S)
        url_list = list(set(url_list))
        num = 1
        for i in url_list:
            i = 'http://www.se5y.info/' + i
            html2 = requests.get(i).text
            picUrlList = re.findall('http://pic1.988aiai.com/images/+\d+/+\w+.jpeg',html2,re.S)
            picture_url_list.append(list(set(picUrlList)))
            print('图片地址解析中%d...' % num)
            num += 1
        return picture_url_list

    # 根据图片url下载图片到本地
    def fetch_img(self, path, data_list):
        if not os.path.exists(path):
            os.mkdir(path)
        x = 1
        m = n
        for e in data_list:
            for pic in e:
                picPath = path + str(100 * m + x) + pic[-5:]
                ir = requests.get(pic, timeout=15, headers=self.headers)
                while os.path.exists(picPath):
                    m += 1
                    picPath = path + str(100 * m + x) + pic[-5:]
                # 下载图片到本地
                open(picPath, 'wb').write(ir.content)
                print('下载图片%d中...,图片地址：%s' % ((100 * m + x), pic))
                x += 1


if __name__ == '__main__':
    m = 0
    local = ['Y', 'O', 'T', 'Q', 'M', 'S', 'MX', 'K']
    rand = random.choice(local)
    for each in rand:
        n = random.randint(2, 10)
        url = "http://www.se5y.info/se5y-tupianqu/%sSE/index_%d.html" % (each, n)
        path = "D:/picture/se5y/%sse/" % each
        if not os.path.exists(path):
            os.mkdir(path)
        print("=====================================")
        print("数据源网址是：" + url)
        print("存储路径是：" + path)
        print("=====================================")
        ao = Ao3(url)
        picture_path_list = ao.get_pic_list()
        print(picture_path_list)
        ao.fetch_img(path, picture_path_list)
        m += 1
        n += 1

