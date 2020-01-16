#!/usr/bin/env python
# coding:utf-8
# Python Version: Python3
# Author: Bob Lee
# https://github.com/bobleer/Multi_Tables_Join
'''
多 Table Join 小脚本
使用前请确保:
0. 已安装 Pandas
1. 每个表格的第一行是列名
2. 非 Excel 格式的表有明确的分隔符
3. 每个文件只包含一种分隔符...
'''

import os
import sys
import pandas as pd
from copy import deepcopy


def identifyFileType(filepath):
    file = os.path.basename(filepath)
    fileName, typeName = os.path.splitext(file)
    return typeName


def identifySep(filepath, tableType):
    if "xls" in tableType: 
        return ""
    else:
        firstLine = open(filepath, 'r').readline().strip()
        if '\t' in firstLine: return "\t"
        elif ',' in firstLine: return ','
        elif ' ' in firstLine: return ' '
        elif '|' in firstLine: return '|'


def openTable(filepath, tableSep):
    if tableSep == "":
        return pd.read_excel(filepath)
    else:
        return pd.read_csv(filepath, sep=tableSep)


def multiJoin(tableDFList):
    while len(tableDFList) > 1:
        table1, table2 = eval(tableDFList.pop()), eval(tableDFList.pop())
        joinedTable = pd.merge(table1, table2, how="outer", copy=0, sort=0)
        tableDFList.append("joinedTable")
    else:
        joinedTable = eval(tableDFList[0])
    return joinedTable


def originalTbalesStatistic(joinedTable, tableDFList):
    
    allColumns = sum([eval(i).columns.to_list() for i in tableDFList],[])
    primaryColumns = [i for i in joinedTable.columns if allColumns.count(i) > 1]
    everyColumnDistinctCount = []
    for index, i in enumerate(tableDFList[::-1]):
        thisTbale = eval(i)
        everyPrimaryColumnsDistinctCount = [thisTbale[k].drop_duplicates().shape[0] for k in primaryColumns]
        specialColumns = [j for j in thisTbale.columns if j not in primaryColumns]
        everySpecialColumnsDistinctCount = [thisTbale[k].drop_duplicates().shape[0] for k in specialColumns]
        everyColumnDistinctCount.append("\t".join(map(str,everyPrimaryColumnsDistinctCount+['']*index+everySpecialColumnsDistinctCount)))
    return "\n".join(everyColumnDistinctCount)


def joinedTableStatistic(joinedTable):
    countAllTable = joinedTable.shape[0]
    everyColumnDistinctCount = [countAllTable]+[joinedTable[i].drop_duplicates().shape[0] for i in joinedTable.columns]
    return "\t".join(map(str,everyColumnDistinctCount))


# 输入多个Tables路径并打开
tableListInput = [i.strip() for i in sys.argv[1:]][::-1]
tableDFList = []
for index, filepath in enumerate(tableListInput):
    tableType = identifyFileType(filepath)
    tableSep = identifySep(filepath, tableType)
    varName = "tableDF_" + str(index)
    tableDFList.append(varName)
    locals()[varName] = openTable(filepath, tableSep)

# 合并多个Tables
tableDFList_bak = deepcopy(tableDFList)
joinedTable = multiJoin(tableDFList)

# 生成统计信息
statisticInfo = originalTbalesStatistic(joinedTable, tableDFList_bak) + '\n\n' + joinedTableStatistic(joinedTable)

# 结合统计信息&最终表
combinedAll = statisticInfo + "\n" + joinedTable.to_csv(sep="\t")

# 输出
print(combinedAll)