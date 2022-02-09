# -*- coding: utf-8 -*- 
# @Time : 2021/5/18 9:02 
# @Author : chang
# @File : pp.py
import pandas
import os
import datetime
import sqlalchemy.sql.default_comparator
import sqlalchemy
import time

def change_excel_pisdata(excelname,pisexceltime):
    """
    修改pis上的数据
    :param excelname:excel的name，提供os提取传入此函数上
    :return: 修改后的datafarme pis data1
    """
    pis_data = pandas.read_excel('''C:\\pp\\{name} '''.format(name=excelname), header=3)

    exceltime=str(pisexceltime)

    start_time = datetime.datetime.strptime(exceltime, '%Y%m%d')


    pis_data = pandas.DataFrame.from_records(pis_data, columns=['功能厂', '制程', '颜色', '负责主管', '总良品','总入库','总NG'])
    pis_data = pis_data.rename(columns={'总NG': '总ng'})
    pis_data['time'] = start_time
    print(pis_data)
    return pis_data


def chang_excel_ppdata(excelname,sapexceltime):
    """
    change ppexcel runturn pp_excel
    :param excelname: .
    :return: .
    """
    print('excel名为：')
    print('''----------------C:\\pp\\{name} '''.format(name=excelname))
    ppdata = pandas.read_excel('''C:\\pp\\{name} '''.format(name=excelname))


    ppdata = pandas.DataFrame.from_records(ppdata,
                                           columns=['Order', 'Type', 'Material', 'Cfmed Yield',
                                                    'Cfmed Scr', 'Actual Usage', 'Component',  'Uni', 'SLoc',
                                                    'TECO Date'])
    # ppdata['TECO Date'] = datetime.datetime.strptime(str(sapexceltime), '%Y%m%d')
    ppdata = ppdata.rename(
        columns={'Order': 'Order', 'Type': 'Type', 'Material': 'Material',
                 'Cfmed Yield': 'CYield', 'Cfmed Scr': 'CScr',  'Actual Usage':'Actual Usage', 'Component':'Component',
                 'Uni': "Uni", 'SLoc': 'SLoc','TECO Date': 'TEDate'})
    print(ppdata)
    return ppdata


if __name__ == '__main__':
    # try:
    path = 'C:\\pp'
    file_list = os.listdir(path)
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.max_rows', None)
    excel_name_o = file_list[0]
    excel_name_t = file_list[1]

    a = excel_name_o.replace(excel_name_o[3:11], '')
    excel_name_o_time=excel_name_o[3:11]
    excel_name_t_time=excel_name_t[3:11]
    print('文件名为：')
    print(excel_name_o)
    print(excel_name_t)

    engine = sqlalchemy.create_engine('postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1')

    if a == 'pis.xlsx':
        pis_excel = change_excel_pisdata(excelname=excel_name_o,pisexceltime=excel_name_o_time)
        pp_excel = chang_excel_ppdata(excelname=excel_name_t,sapexceltime=excel_name_t_time)
    #
    else:
        pis_excel = change_excel_pisdata(excelname=excel_name_t,pisexceltime=excel_name_t_time)
        pp_excel = chang_excel_ppdata(excelname=excel_name_o,sapexceltime=excel_name_o_time)
    pis_excel.to_sql('pp_pis_data', engine, index=False, if_exists='append')
    pp_excel.to_sql('pp_sap_data', engine, index=False, if_exists='append')
    print('录入成功')
    # except Exception as e:
    #     print(e)
    #     print('录入失败，请检擦excel格式')
    time.sleep(10)
