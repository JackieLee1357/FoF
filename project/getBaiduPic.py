#!usr/bin/python
# coding=utf-8
"""根据搜索词下载百度图片"""
import re
import os
import time
import urllib
import requests
from fake_useragent import UserAgent


def getPage(keyWord, page, n):
    page = page * n
    keyWord = urllib.parse.quote(keyWord, safe='/')
    url_begin = "http://image.baidu.com/search/flip?tn=baiduimage&ie=utf-8&word="
    url1 = url_begin + keyWord + "&pn=" + str(page) + "&gsm=" + str(hex(page)) + "&ct=&ic=0&lm=-1&width=0&height=0"
    return url1


def get_onepage_urls(onepageurl):
    try:
        html = requests.get(onepageurl, headers=headers).text
    except Exception as e:
        print(e)
        pic_urls = []
        return pic_urls
    pic_urls = re.findall('"objURL":"(.*?)",', html, re.S)
    return pic_urls


def down_pic(pic_urls):
    """给出图片链接列表, 下载所有图片"""
    for i, pic_url in enumerate(pic_urls):
        try:
            pic = requests.get(pic_url, timeout=15, headers=headers)
            time.sleep(1)
            path = basePath + keyword + '/'
            if not os.path.exists(path):
                os.mkdir(path)
            string = path + str(i + 1) + '.jpg'
            with open(string, 'wb') as f:
                f.write(pic.content)
                print('成功下载第%s张图片: %s' % (str(i + 1), str(pic_url)))
        except Exception as e:
            print('下载第%s张图片时失败: %s' % (str(i + 1), str(pic_url)))
            print(e)
            continue


if __name__ == '__main__':
    basePath = "D:/picture/baiduPicture/"
    try:
        if not os.path.exists(basePath):
            os.mkdir(basePath)
    except:
        basePath = "C:/picture/baiduPicture/"
        if not os.path.exists(basePath):
            os.mkdir(basePath)
    keyword = input('请输入搜素关键词：')  # 关键词, 改为你想输入的词即可, 相当于在百度图片里搜索一样
    page_begin = 0
    page_number = int(input('请输入要下载的页数：'))
    image_number = 3
    ua = UserAgent
    headers = {"User-Agent": str(ua)}
    all_pic_urls = []
    print('存储路径为： ' + basePath)
    while 1:
        if page_begin > image_number:
            break
        print("第%d次请求数据", [page_begin])
        url = getPage(keyword, page_begin, page_number)
        onePage_urls = get_onepage_urls(url)
        page_begin += 1
        all_pic_urls.extend(onePage_urls)
    down_pic(list(set(all_pic_urls)))
    print('\r\n')
    os.system('pause')
