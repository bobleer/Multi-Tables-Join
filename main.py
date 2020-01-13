#!/usr/bin/env python
# coding:utf-8
# Python Version: Python3
# Author: Bob Lee
# https://github.com/bobleer/Multi_Tables_Join
'''
多Table Join小脚本
使用前请确保:
0. 已安装 Pandas
1. 每个表格的第一行是列名
2. 非 Excel 格式的表有明确的分隔符
3. 每个文件只包含一种分隔符...
'''

import os
import sys
import pandas as pd


def identifyFileType(filepath):
    file = os.path.basename(filepath)
    fileName, typeName = os.path.splitext(file)
    return typeName

def identifySep(filepath, tableType):
    if "xls" in tableType: 
        return ""
    else:
        open(resultlist[0],'r').readline().strip()


def openTable():

def multiJoin(tableDFList):
    while len(tableDFList) > 1:
        table1, table2 = eval(tableDFList.pop()), eval(tableDFList.pop())
        joinedTable = pd.merge(table1, table2, how="outer", copy=0, sort=0)
        tableDFList.append("joinedTable")
    else:
        joinedTable = eval(tableDFList[0])
    return joinedTable

def doStatistic(joinedTable):

    return statisticInfo

# 输入多个Tables并打开
tableListInput = [i.strip() for i in sys.argv[1:]][::-1]
tableDFList = []
for index, filepath in enumerate(tableListInput):
    tableType = identifyFileType(filepath)
    tableSep = identifySep(filepath, tableType)
    varName = "tableDF_" + str(index)
    tableDFList.append(varName)
    locals()[varName] = openTable(filepath, tableSep)

# 合并多个Tables
joinedTable = multiJoin(tableDFList)

# 生成统计信息
statisticInfo = doStatistic(joinedTable)

# 结合统计信息&最终表
combinedAll = statisticInfo + joinedTable.to_csv(sep="\t")

# 输出
print(combinedAll)