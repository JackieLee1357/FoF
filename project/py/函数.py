import xlwt
import pyodbc
import pandas
import datetime
import configparser
import time
import xlwings
# app=xlwings.App(visible=False,add_book=False)
# file_name = datetime.datetime.now().strftime('%y%m%d%H%M') + '.xls'
# workbook=app.books.add()
# workbook.save(file_name)
# workbook.close()
# app.quit()

def new_workbook(file_name1):
    app=xlwings.App(visible=False,add_book=False)
    workbook=app.books.add()
    workbook.save(file_name1)
    workbook.close()
    app.quit()


x=datetime.datetime.now().strftime('%y%m%d%H%M') + '.xls'
new_workbook(x)

config = configparser.ConfigParser()
config.read('c:\\pc\\tsxt.ini')
a = config.get('cmd', 'id')
b = config.get('cmd', 'id2')
x = config.get('cmd','id2')

def link_sql(link):
    pyodbc.connect(link)



x=10086



