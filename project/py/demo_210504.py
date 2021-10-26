# -*- coding: utf-8 -*-
# @Time : 2021/5/5 13:53
# @Author : chang
# @File : demo_210504.py
import time
import datetime
a = "2021年5月7日"
start = datetime.datetime.strptime(a,'%Y年%m月%d日')#注意这里用的是年月日隔开
print(start)



# def bubbleSort(arr):
#     for i in range(1, len(arr)):
#         print('i',i)
#         for j in range(0, len(arr)-i):
#             print('j',j)
#             if arr[j] > arr[j+1]:
#                 arr[j], arr[j + 1] = arr[j + 1], arr[j]
#     return arr
#
# a=[1,2,3,6,34,32,3,4,5,6]
# print(bubbleSort(a))
# #
# #
# #
# def selectionSort(arr):
#     for i in range(len(arr) - 1):
#         # 记录最小数的索引
#         minIndex = i
#         for j in range(i + 1, len(arr)):
#             if arr[j] < arr[minIndex]:
#                 minIndex = j
#         # i 不是最小数时，将 i 和最小数进行交换
#         if i != minIndex:
#             arr[i], arr[minIndex] = arr[minIndex], arr[i]
#     return arr
# import os
#
#
# def work_file(file):
#     for root, dirs, files in os.walk(file):
#
#         for f in files:
#             print('a'+os.path.join(root, f))
#         for d in dirs:
#             print(os.path.join(root, d))
#
#     # 遍历所有的文件夹
#
#
#
# if __name__ == '__main__':
#     work_file("D:/存档")
