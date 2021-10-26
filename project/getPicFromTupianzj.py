import fake_useragent
import requests
from lxml import etree
from fake_useragent import UserAgent
import os


class Ao3:
    def __init__(self):
        self.jiandan_url = "https://www.tupianzj.com/meinv/xinggan/"
        self.ua = fake_useragent.UserAgent(path='C:/Users/Administrator/AppData/Local/Temp/fake_useragent.json')
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": self.ua.random,
            "referer": "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_jiandan_list(self):
        picturl_path_list = []
        jiandan_url = self.jiandan_url
        # 访问主网页
        res = requests.get(jiandan_url, headers=self.headers)
        res.encoding = 'utf-8'
        # print(res.text)
        root = etree.HTML(res.text)
        print("root---------------------------------------")
        # print(root)
        content = root.xpath('//*[@id="container"]/div/div/div[3]/div/ul/li/a/img/@src')
        print("xpath---------------------------------------")
        # print(content)
        for each in content:
            # print(each)
            picturl_path_list.append(each)
        print("---------------")
        print(picturl_path_list)
        return picturl_path_list

    # 根据图片url下载图片到本地
    def fetch_img(self, path, data_list):
        if not os.path.exists(path):
            os.mkdir(path)

        x = 0
        for pic in data_list:
            # pic = "https://info.xitek.com/" + pic
            print(pic)
            ir = requests.get(pic)
            # 下载图片到本地
            open(path + str(x) + pic[-4:], 'wb').write(ir.content)
            x += 1


if __name__ == '__main__':
    ao = Ao3()
    path1 = "D:/picture/tupianzj/"
    path = ''
    if not os.path.exists(path1):
        os.mkdir(path1)
        path = path1
    else:
        for i in range(len(path1) - 1):
            path += path1[i]
    path = path + '-new/'
    picture_path_list = ao.get_jiandan_list()
    picture_path_list = list(set(picture_path_list))
    print("=====================================")
    print(picture_path_list)
    print("=====================================")
    ao.fetch_img(path, picture_path_list)
