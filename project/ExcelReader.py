import xlrd

file = r'D:\PythonPy\7-17.xlsm'
data = xlrd.open_workbook(file)
sheets = data.sheet_names()
print(sheets)
table = data.sheet_by_name(sheets[4])
name = table.name
rowNum = table.nrows
cloNum = table.ncols

print(name)
print(rowNum)
print(cloNum)

val1 = table.cell_value(1,1)
print(type(val1))
print(val1)

row1Val = table.row_values(1)
print(row1Val)