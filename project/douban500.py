import requests
from lxml import etree
from chardet import detect
from pymongo import MongoClient
from selenium import webdriver
from fake_useragent import UserAgent
import xlwt

def douban_spider(sheet):
    """
    豆瓣爬虫调度器
    :return: None
    """
    headers = {
        "User-Agent": UserAgent().random,
        "referer":  "https://www.zhihu.com/"
    }

    # 总公有4页,每页间隔25
    for i in range(0, 101, 25):
        # 用 requests 发送请求获取 html 文档
        url = 'https://www.douban.com/doulist/13704241/?start=' + str(i)
        print(url)
        response = requests.get(url, headers=headers)
        # 用 xpath 规则解析 html 文档
        html = response.content.decode(detect(response.content).get('encoding'))
        tree = etree.HTML(html)
        page_parser(tree,sheet)


def page_parser(tree,sheet):
    """
    页面解析器
    :return:
    """
    for item in tree.xpath('//div[@class="article"]/div[@class="doulist-item"]'):
        data = dict()
        # 排名
        data['ranking'] = item.xpath('.//div[@class="hd"]/span/text()')[0]
        # 标题
        data['title'] = ''.join(item.xpath('.//div[@class="bd doulist-subject"]/div[@class="title"]/a/text()')).strip()

        abstract = item.xpath('.//div[@class="abstract"]/text()')
        if abstract:
            # 导演
            data['director'] = ''.join(abstract[0]).strip().split(':')[-1].strip()
            # 上映年份
            try:
                data['year'] = ''.join(abstract[4]).strip().split(':')[-1].strip()
            except Exception as e:
                data['year'] = ''.join(abstract[3]).strip().split(':')[-1].strip()
        # 评分
        rating_num = item.xpath('.//span[@class="rating_nums"]/text()')
        if rating_num:
            data['rating_num'] = rating_num[0]
        print(data)
        save_data(data,sheet)


def save_data(data,sheet):
    """
    将爬取的数据写入 mongodb 数据库
    :return: bool 数据是否保存成功
    """
    
    i = 0
    for key in data:
        j = int(data['ranking'])
        print(data[key])
        print("(=======)")
        if j == 1:
            sheet.write(j-1,i,key)
        sheet.write(j,i,data[key])
        i += 1
    


def main(sheet):
    douban_spider(sheet)


if __name__ == '__main__':
    adata = xlwt.Workbook(encoding= 'utf-8')
    sheet = adata.add_sheet('sheet2')
    main(sheet)
    adata.save('test.xls')