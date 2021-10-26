import xlrd
import matplotlib.pyplot as plt

def removeSpace(aList):
    while aList[-1] == '':
        aList.remove(aList[-1])
    return aList


def count(aSheet, section):
    num = 0
    for i in range(aSheet.nrows):
        if section == aSheet.row_values(i)[-2]:
            num += aSheet.row_values(i)[-5]
    return num


if __name__ == '__main__':
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    data = xlrd.open_workbook('生产Trace漏失数据报表-8.1.xlsm')
    sheet = data.sheet_by_name('数据')
    a = count(sheet, '管理问题')
    b = count(sheet, '机台问题')
    c = count(sheet, '软件问题')
    d = count(sheet, '制程问题')
    dic = {'管理问题': a, '机台问题': b, '软件问题': c, '制程问题': d}
    dict = {}
    for i, d in dic.items():
        if not d == 0:
            dict.update({i: d})
    x = [value for value in dict.values()]
    y = [key for key in dict]
    color = ['r', 'b', 'w', 'y']
    exd = [0, 0.05, 0, 0]
    color = color[:len(dict)]
    exd = exd[:len(dict)]
    figure = plt.figure()
    plt.title('各问题点占比')
    aPie = plt.pie(x, labels=y, shadow=True, radius=1.3, autopct='%1.2f%%', explode=exd, colors=color, startangle=90,
                   pctdistance=0.5)
    plt.savefig('pie.jpg')
    #plt.show()
    plt.close()

    blocked1 = sheet.col_values(-5)
    blocked = []
    for i in blocked1:
        try:
            blocked.append(int(i))
        except:
            continue
    blocked.sort()
    x = blocked[-5:]
    x = x[::-1]
    y = []
    for j in x:
        for i in range(sheet.nrows):
            if j == sheet.row_values(i)[-5]:
                y.append(sheet.row_values(i)[1])

    print(y)
    rightAxis = plt.subplot

    #aBar = plt.bar(x=y, height=x, width=0.6, color='r')
    #plt.show()
    sum = 0
    for i in x:
        sum += i
    y = []
    for i in x:
        i /= sum
        j = format(i, '.2f')
        print(j)
        y.append(float(j))
    print(y)
    x = [1, 2, 3, 4, 5]
    aPlot = plt.plot(x, y)
    plt.show()
