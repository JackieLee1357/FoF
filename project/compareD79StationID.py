import xlrd
import xlwt


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
            print(e)

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
        rows.append(r)
    return rows


def compareStationIDD79(fileName1, fileName2):
    list1 = getAllAgents(fileName1, 'Agents List')
    list2 = getDataSource(fileName2, 4)
    errorStation1 = []
    errorStation = []
    for each in list2:
        if each[3] == 'agent':
            each[0] = 'ID'
            each[4] = 'addr'
            each[5] = 'line ID'
            each[6] = 'station ID'
            each[7] = 'ErrorType'
            each[8] = 'ErrorType'
            each[9] = 'Error/Correct'
            each[10] = 'Error/Correct'
            errorStation.append(each)
        for agent in list1:
            if each[3] in agent:
                if not each[4] == agent[9]:
                    each[7] = 'LineIDError'
                    each[9] = each[5] + ' / ' + agent[9]
                    each[4] = agent[4]
                    errorStation.append(each)
                if not each[5] == agent[10]:
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


if __name__ == '__main__':
    fileNameSourceD79 = 'D:/data/1000pcs数据.xlsx'
    fileNameAllD79 = 'D:/data/JWX_D79_Agent_list 2020730 Tenshi v1.xlsx'
    fileNameErrorAgentD79 = 'D:/data/D79errorIDMsg.xls'
    errorIDMsgS = compareStationIDD79(fileNameAllD79, fileNameSourceD79)
    errorIDMsg = xlwt.Workbook(encoding='utf-8')
    errorMsg = errorIDMsg.add_sheet('errorIDMsg')
    row = 0
    col = 0
    for ro in errorIDMsgS:
        for co in ro:
            if not row == 0 and col == 0:
                errorMsg.write(row, col, row)
            else:
                errorMsg.write(row, col, co)
            col += 1
        row += 1
        col = 0
    errorIDMsg.save(fileNameErrorAgentD79)
