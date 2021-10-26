
import requests
from lxml import etree
#coding:utf-8
import unittest
from selenium import webdriver
from bs4 import BeautifulSoup


class seleniumTest(unittest.TestCase):
    def setUp(self):
        self.driver = webdriver.PhantomJS(executable_path=r'D:\PhantomJs\phantomjs-2.1.1-windows\bin\phantomjs.exe')

    def testEle(self):
        driver = self.driver
        driver.get('https://tieba.baidu.com/f?kw=python&ie=utf-8')
        soup = BeautifulSoup(driver.page_source, 'xml')
        while True:
            titles = soup.find_all('h3', {'class': 'ellipsis'})
            nums = soup.find_all('span', {'class': 'dy-num fr'})
            for title, num in zip(titles, nums):
                print(title.get_text(), num.get_text())
            if driver.page_source.find('shark-pager-disable-next') != -1:
                break
            elem = driver.find_element_by_class_name('shark-pager-next')
            elem.click()
            soup = BeautifulSoup(driver.page_source, 'xml')
            print(soup)

    def tearDown(self):
        print('down')

    


class getInfoFromBaidutieba:

    def __init__(self):
        self.url = 'https://tieba.baidu.com/f?kw=python&ie=utf-8'
        self.ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87'
        self.headers = {
            # 获取随机的User-Agent
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87',
            "referer":  "https://www.zhihu.com/"
        }

    def getHtml(self):
        url = self.url
        response = requests.get(url,headers = self.headers)
        print('xPath=====================================')
        print(response.text)
        result = etree.HTML(response.text).xpath('//*[@id="thread_top_list"]/li/div/div[2]/div/div/a/@herf')
        print('=====================================')
        print(result)


if __name__ == "__main__":
    unittest.main()
    getInfo = getInfoFromBaidutieba()
    #list = getInfo.getHtml()
    #print(list)

    
