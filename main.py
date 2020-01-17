#!/usr/bin/env python
# coding:utf-8
# Python Version: Python3
# Author: Bob Lee
# https://github.com/bobleer/Multi_Tables_Join
# 2020-01-17 16:30:31
'''
多 Table Join 小脚本
使用前请确保:
0. 已安装 Pandas
1. 每个表格的第一行是列名
2. 非 Excel 格式的表有明确的分隔符
3. 每个文件只包含一种类型的分隔符...
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


def getPrimaryColumns(tableDFList, joinedTable):
    allColumns = sum([eval(i).columns.to_list() for i in tableDFList],[])
    primaryColumns = [i for i in joinedTable.columns if allColumns.count(i) > 1]
    return primaryColumns

def originalTbalesStatistic(tableDFList, joinedTable):
    originalColumnDistinctCount = []
    for index, table in enumerate(tableDFList[::-1]):
        thisTable = eval(table)
        thisTableStatistic = []
        for column in joinedTable.columns:
            try:
                thisTableStatistic.append("%s: %s"%(column,thisTable[column].drop_duplicates().shape[0]))
            except:
                thisTableStatistic.append("")
        originalColumnDistinctCount.append("\t".join(map(str,[thisTable.shape[0]]+thisTableStatistic)))

    return "\n".join(originalColumnDistinctCount)


def joinedTableStatistic(joinedTable):
    countAllTable = joinedTable.shape[0]
    joinedColumnDistinctCount = [countAllTable]+[joinedTable[i].drop_duplicates().shape[0] for i in joinedTable.columns]
    return "\t".join(map(str,joinedColumnDistinctCount))


# 输入多个Tables路径 && 检查文件类型 && 判断分隔符 && 打开文件
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

# 空值转NULL && 根据SpecialColumns[::-1]倒序排序
primaryColumns = getPrimaryColumns(tableDFList_bak, joinedTable)
joinedTable_ordered = joinedTable.fillna(value="NULL").sort_values(by=[i for i in joinedTable.columns.to_list()[::-1] if i not in primaryColumns], ascending=False).reset_index(drop=True)

# 生成每个表以及最终表的统计信息
statisticInfo = originalTbalesStatistic(tableDFList_bak, joinedTable_ordered) + '\n\n' + joinedTableStatistic(joinedTable_ordered)

# 合并统计信息和最终表结果
combinedAll = statisticInfo + "\n" + joinedTable_ordered.to_csv(sep="\t")

# 输出
print(combinedAll.rstrip(),end='')