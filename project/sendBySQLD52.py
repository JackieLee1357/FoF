import xlwt
import yagmail
import pymssql


def getDataFromSQL():
    dataList = []
    connect = pymssql.connect('localhost', 'jack', '123456', 'Trace')  # 建立连接
    if connect:
        print("连接成功!")
    cursor = connect.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
    sql = '''
SELECT DISTINCT e.process as process, f.[agent] as agent, e.addr as IP,  e.station_id as correctStationID,  f.[stationid] as errorStationID 
FROM [Trace].[dbo].[JWX_ D52_Agent list] e 
inner join [Trace].[dbo].[RPT_Choice] f 
on e.agent_id=f.[agent] and e.station_id<>f.[stationid] AND F.[project]='D52'
order by e.process
'''
    cursor.execute(sql)  # 执行sql语句
    row = cursor.fetchone()  # 读取查询结果,
    while row:  # 循环读取所有结果
        row1 = list(row)
        dataList.append(row1)
        row = cursor.fetchone()

    cursor.close()
    connect.close()
    row0 = ['process', 'agent', 'IP', 'correctStationID', 'errorStationID']
    dataList.insert(0,row0)
    return dataList


def autoSendMail(file):
    yag = yagmail.SMTP(user='Yuan_Li5928@qq.com', password='pafeotuillffbeje', host='smtp.qq.com')
    receiver = ['Finlay_Xu@Jabil.com', 'weiwei_huang1@jabil.com', 'Wendy_Kong@jabil.com', 'Chun_Zhong@Jabil.com', 'Bernie_Wang@Jabil.com', 'Charlie_Zeng@Jabil.com', 'Glenn_Yang@jabil.com', 'Tenshi_Zhang@jabil.com', 'wxisme@jabil.onmicrosoft.com']
    receiver = ['yuan_li5928@jabil.com']
    title = 'Station ID配置错误机台信息'
    body = '''
        Hi Finlay & weiwei,

            附件为昨晚12：00至今Trace机台上传数据与客户配置的Agent List对比Station ID不符的机台信息，请确认并改正，谢谢。


        Best Regards,
        YUAN LI 李源
        Expert Technician
        Operation Excellence,Wuxi Metal, Jabil Green Point

        Cell1: +86 186-7210-1357(CN) 
        Desk: +86 510-8189-6258
        JVN: 82-994-6258
        Email: Yuan_Li5928@jabil.com
        Block B State, High-Tech Industry Development Zone,
        Wuxi, Jiangsu, 214112, China
        www.jabil.com

        The information contained in this e-mail message (and any attachment transmitted herewith) is for the exclusive use
        of the intended addressee(s) only and is confidential information. If you have received this electronic message in error,
        please reply to the sender highlighting the error and destroy the original message and all copies immediately.
        本郵件（包括其附件）所含信息為保密信息，僅為特定收件人之用，如您誤收此郵件，請立即通知發件人，並将郵件銷毀。
        '''
    try:
        yag.send(to=receiver, subject=title, contents=[body, file])
        print('发送邮件成功')
    except Exception as e: 
        print(e)
        files = [body]
        for i in file:
            files.append(i)
        yag.send(to=receiver, subject=title, contents=files)
        print('发送邮件成功')


def saveFile(fileName, errorList):
    errorIDMsg = xlwt.Workbook(encoding='utf-8')
    errorMsg = errorIDMsg.add_sheet('errorIDMsg')
    row = 0
    col = 0
    for ro in errorList:
        for co in ro:
            errorMsg.write(row, col, co)
            col += 1
        row += 1
        col = 0
    errorIDMsg.save(fileName)


if __name__ == '__main__':
    fileNameErrorAgentD52 = 'D:/data/D52errorIDMsg.xls'
    errorIDMsgS = getDataFromSQL()
    saveFile(fileNameErrorAgentD52, errorIDMsgS)
    #savedFiles = [fileNameErrorAgentD52, fileNameErrorAgentD79]
    savedFiles = [fileNameErrorAgentD52]
    autoSendMail(savedFiles)
