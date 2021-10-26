"""
Version: Python3.5
Author: OniOn
Site: http://www.cnblogs.com/TM0831/
Time: 2018/12/27 14:49

正则表达式实例
"""
import re

class regular:

    def __init__(self):
        self.name = ""

    # \\d+ : 代表多个数字
    def get_number(self, str):
        # 找出<em>XXX</em>中的所有数字
        # () -- 指只要中间的数字  不加() -- 指所有的内容，包含<em>XXXX</em>
        restr = "<em>(\\d+)</em>"
        reget = re.findall(restr,str)
        return reget

    # 身份证截取
    def get_sfz(self):
        s = '1102231990xxxxxxxx'
        res = re.search('(?P<province>\d{3})(?P<city>\d{3})(?P<born_year>\d{4})',s)
        print(res.groupdict())

    def get_outlook(self):
        pattern = re.compile("[\w+\.]+@[a-zA-Z\d]+\.(com|cn)")

        pattern = re.compile("""[\w+\.]+  # 匹配@符前的部分
                                    @  # @符
                                    [a-zA-Z\d]+  # 邮箱类别
                                    \.(com|cn)   # 邮箱后缀  """, re.X)
        print(pattern.findall('1572452653@qq.com'))



if __name__ == '__main__':
    reg = regular()

    pattern = "<div>.\\d+([\u4e00-\u9fa5]+)\\d+</div>"
    res = re.findall(pattern,'<div>12你好啊111你222不好</div>')
    # res = re.findall(pattern,'<div>你好啊你不好</div>')
    print(res)

    # str = "<ssjh>ss623<djd><em>7364</em>hdsjd><em>7364</em>hdsfdajdjd><em>7364</em>hdscweashjvuydfe"
    # reget = reg.get_number(str)
    # print(reget)
    # print(reget[0])

    # reg.get_sfz()
    # str = 'shgd271673553467'
    # res = re.search('(?P<province>\d{3})',str)
    # print(res.groupdict())

    # line = "Cats are smarter than dogs";
    # searchObj = re.search( r'(.*)are(.*?).*', line)
    # if searchObj:
    #     print ("searchObj.group() : ", searchObj.group())
    #     print ("searchObj.group(1) : ", searchObj.group(1))
    #     print ("searchObj.group(2) : ", searchObj.group(2))
    # else:
    #     print ("Nothing found!!")
