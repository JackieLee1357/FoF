import fake_useragent
import img2pdf
import requests
from bs4 import BeautifulSoup
from lxml import etree
import os
from fake_useragent import UserAgent


class PictureCatcher:
    def __init__(self, url):
        self.url = url
        self.ua = fake_useragent.UserAgent(path='C:/Users/Administrator/AppData/Local/Temp/fake_useragent.json')
        self.headers = {
            "User-Agent": self.ua.random,    # 获取随机的User-Agent
            "referer": "https://www.zhihu.com/"
        }

    # 获取照片地址列表
    def get_pic_list(self):
        url1 = self.url
        # 访问主网页
        res = requests.get(url1, headers=self.headers)
        res.encoding = 'utf-8'
        print('response==============================================================================================')
        print(res.text)
        html = etree.HTML(res.text)
        print("html---------------------------------------")
        print(html)
        urlList = html.xpath(
            '//*[@id="js_content"]//img/@data-src')  # //*[@id="ssr-content"]/div[2]/div[2]/div[1]/div[1]//@src  //*[@id="js_content"]/section[5]/section/section/section/p[1]/img[1]
        print("urlList---------------------------------------")
        print(urlList)
        print('地址解析完成')
        print('----------')
        return urlList

    # 根据图片url下载图片到本地
    @staticmethod
    def fetch_img(path1, data_list):
        if not os.path.exists(path1):
            os.mkdir(path1)
        picPathList = []
        print(path1)
        for x in range(len(data_list)):
            pic = data_list[x]
            picStyle = pic.split("=")[-1]
            picPath = path1 + str(x) + "." + picStyle
            ir = requests.get(pic)
            # while os.path.exists(picPath):
            #     picPath = path + str(x) + "x." + pic[-4:]    # 如果存在图片，重命名
            # 下载图片到本地
            open(picPath, 'wb').write(ir.content)
            print('下载图片%d中...' % x)
            picPathList.append(picPath)
        to_pdf(picPathList)
        print('图片下载完成')
        print('----------')


def to_pdf(picList):
    a4input = (img2pdf.mm_to_pt(1080), img2pdf.mm_to_pt(608))
    layout_fun = img2pdf.get_layout_fun(a4input)
    with open(path + 'a.pdf', 'wb') as f:
        f.write(img2pdf.convert(picList, layout_fun=layout_fun))  # 批量导入PDF


if __name__ == '__main__':
    try:
        url = "https://mp.weixin.qq.com/s/OYP2UKUEQi12VJVEmAP6TA"
        path = "D:\picture\weixin9/"
        if not os.path.exists(path):
            os.mkdir(path)
        print("=====================================")
        print("数据源网址是：" + url)
        print("存储路径是：" + path)
        print("=========================== ==========")
        ao = PictureCatcher(url)
        picturePathList = ao.get_pic_list()
        picturePathList = list(picturePathList)
        ao.fetch_img(path, picturePathList)
    except Exception as e:
        print(str(e))
