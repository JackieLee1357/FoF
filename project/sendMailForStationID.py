import xlrd
import xlwt
import yagmail

def getDataSource(fileName, index):
    sheet1 = xlrd.open_workbook(fileName).sheet_by_index(0)
    n = sheet1.nrows - 1
    rows = []
    for i in range(0, n):
        r = sheet1.row_values(i)
        r[-1] = ''
        r.append('')
        r.append('')
        r.append('')
        try:
            int(r[0])
            r[0] = ''
        except Exception as e:
            print('')

        if not r in rows:
            r.insert(index, '')
            rows.append(r)
    return rows

def getAllAgents(fileName, sheetName):
    sheet = xlrd.open_workbook(fileName).sheet_by_name(sheetName)
    n = sheet.nrows - 1
    rows = []
    for i in range(0, n):
        if i == 0:
            continue
        r = sheet.row_values(i)
        r[6] = r[6].lower()
        rows.append(r)
    return rows


def compareStationIDD52(fileName1, fileName2):
    list1 = getAllAgents(fileName1, 'Agents List total')
    list2 = getDataSource(fileName2, 3)
    #print(list2)
    errorStation = []
    errorStation1 = []
    for each in list2:
        if each[2] == 'agent':
            each[0] = '序号'
            each[3] = 'addr'
            each[5] = '机台上传的Station ID'
            each[4] = '正确的Station ID'
            errorStation.append(each)
        for agent in list1:
            if each[2] in agent:
                '''if not str(each[4]).strip().__eq__(str(agent[12]).strip()):
                    each[6] = 'LineIDError'
                    each[8] = each[4] + ' / ' + agent[12]
                    each[3] = agent[7]
                    errorStation.append(each)'''
                if not str(each[5]).strip().__eq__(str(agent[15]).strip()):
                    each[3] = agent[7]
                    each[4] = agent[15]
                    errorStation.append(each)
    errors = []
    for e in errorStation:
        if not e[2] in errors:
            errorStation1.append(e)
            errors.append(e[2])
    #errorStation1.sort()
    return errorStation1

def compareStationIDD79(fileName1, fileName2):
    list1 = getAllAgents(fileName1, 'Agents List')
    list2 = getDataSource(fileName2, 4)
    errorStation1 = []
    errorStation = []
    for each in list2:
        each[7] = ''
        if each[3] == 'agent':
            each[0] = 'ID'
            each[4] = 'addr'
            each[5] = 'line ID'
            each[6] = 'station ID'
            each[7] = 'ErrorType'
            each[8] = 'ErrorType'
            each[9] = 'Error / Correct'
            each[10] = 'Error / Correct'
            errorStation.append(each)
        for agent in list1:
            if each[3] in agent:
                if not str(each[5]).strip().__eq__(str(agent[9]).strip()):
                    each[7] = 'LineIDError'
                    each[9] = each[5] + ' / ' + agent[9]
                    each[4] = agent[4]
                    errorStation.append(each)
                if not str(each[6]).strip().__eq__(str(agent[10]).strip()):
                    each[8] = 'StationIDError'
                    each[10] = each[6] + ' / ' + agent[10]
                    each[4] = agent[4]
                    errorStation.append(each)
    errors = []
    for e in errorStation:
        if not e[2] in errors:
            errorStation1.append(e)
            errors.append(e[2])
    return errorStation1

def autoSendMail(file):
    yag = yagmail.SMTP(user='Yuan_Li5928@qq.com', password='xkorxvbdnhefbbcf', host='smtp.qq.com')
    #receiver = ['Finlay_Xu@Jabil.com', 'Wendy_Kong@jabil.com', 'Chun_Zhong@Jabil.com', 'Glenn_Yang@jabil.com', 'Tenshi_Zhang@jabil.com', 'Bernie_Wang@Jabil.com', 'Charlie_Zeng@Jabil.com', 'wxisme@jabil.onmicrosoft.com']
    receiver = ['yuan_li5928@jabil.com']
    title = 'Station ID配置错误机台信息'
    body = '''
        Hi Finlay,

            附件为昨日Trace上传数据与客户配置的Agent List对比Station ID不符的机台信息，请确认并改正，谢谢。


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
            if not row == 0 and col == 0:
                errorMsg.write(row, col, row)
            else:
                errorMsg.write(row, col, co)
            col += 1
        row += 1
        col = 0
    errorIDMsg.save(fileName)

if __name__ == '__main__':
    fileNameSourceD52 = 'D:/data/D52 DATA.xlsx'
    fileNameAllD52 = 'D:/data/JWX_ D52_Agent list -V200820-LiuYang.xlsx'
    fileNameErrorAgentD52 = 'D:/data/D52errorIDMsg.xls'
    '''errorIDMsgS = compareStationIDD52(fileNameAllD52, fileNameSourceD52)
    saveFile(fileNameErrorAgentD52, errorIDMsgS)

    fileNameSourceD79 = 'D:/data/1000pcs数据.xlsx'
    fileNameAllD79 = 'D:/data/JWX_D79_Agent_list 2020730 Tenshi v1.xlsx'
    fileNameErrorAgentD79 = 'D:/data/D79errorIDMsg.xls'
    errorIDMsgS1 = compareStationIDD79(fileNameAllD79, fileNameSourceD79)
    saveFile(fileNameErrorAgentD79, errorIDMsgS1)

    #savedFiles = [fileNameErrorAgentD52, fileNameErrorAgentD79]'''
    savedFiles = [fileNameErrorAgentD52]
    autoSendMail(savedFiles)

