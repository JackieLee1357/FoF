""""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49
爬取煎蛋网页上的图片
"""
import requests
from lxml import etree
import os

class Ao3:
    def __init__(self):
        self.jiandan_url = "http://bbs.fengniao.com/forum/11052026_p99358711.html#post99358711"
        self.ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87'
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87' ,
            "referer":  "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_jiandan_list(self):
        picturl_path_list = []
        jiandan_url = self.jiandan_url
        # 访问主网页
        res = requests.get(jiandan_url, headers=self.headers)
        res.encoding='utf-8'
        #print(res.text)
        root = etree.HTML(res.text)
        content = root.xpath('//*[@class="postMain module1200"]/div/div/div/div/a/img/@src')
        print("xpath---------------------------------------")
        print(content)
        for each in content:
            #print(each)
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
            #pic = "https:" + pic
            pic1 = ''
            for i in pic:
                if i == '?':
                    break
                else:
                    pic1 = pic1 + i
            pic = pic1
            print(pic)
            ir = requests.get(pic)
            # 下载图片到本地
            open(path + str(x) + pic[-5:], 'wb').write(ir.content)
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
    
    

