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


def compareStationIDD52(fileName1, fileName2):
    list1 = getAllAgents(fileName1, 'Agents List total')
    list2 = getDataSource(fileName2, 3)
    errorStation = []
    errorStation1 = []
    for each in list2:
        if each[2] == 'AGENT':
            each[0] = 'ID'
            each[3] = 'addr'
            each[6] = 'ErrorType'
            each[7] = 'ErrorType'
            each[8] = 'Error/Correct'
            each[9] = 'Error/Correct'
            errorStation.append(each)
        for agent in list1:
            if each[2] in agent:
                if not each[5] == agent[12]:
                    each[6] = 'LineIDError'
                    each[8] = each[4] + ' / ' + agent[12]
                    each[3] = agent[7]
                    errorStation.append(each)
                if not each[6] == agent[15]:
                    each[7] = 'StationIDError'
                    each[9] = each[5] + ' / ' + agent[15]
                    each[3] = agent[7]
                    errorStation.append(each)
    errors = []
    for e in errorStation:
        if not e[2] in errors:
            errorStation1.append(e)
            errors.append(e[2])
    print(errors)
    return errorStation1


if __name__ == '__main__':
    fileNameSourceD52 = 'D:/data/D52 DATA.xlsx'
    fileNameAllD52 = 'D:/data/JWX_ D52_Agent list-total 08-06.xlsx'
    fileNameErrorAgentD52 = 'D:/data/D52errorIDMsg.xls'
    errorIDMsgS = compareStationIDD52(fileNameAllD52, fileNameSourceD52)
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
    errorIDMsg.save(fileNameErrorAgentD52)
