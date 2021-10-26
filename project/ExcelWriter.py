import xlwt
data = xlwt.Workbook(encoding= 'utf-8')
sheet = data.add_sheet('sheet1')
sheet.write(0, 0, 'python')
data.save('test.xls')
