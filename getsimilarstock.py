from math import sqrt

import psycopg2

conn = psycopg2.connect(database="finalproject", user="usr",
                        password="*******hidden password******", host="49.234.213.42", port="5432")
cursor = conn.cursor()


def getPercentile(code):
    cursor.execute("select 排名 from percentile where 股票代码=" + str(code))
    rows = cursor.fetchall()
    top = float(int(rows[0][0] * 10)) / 10.0
    bottom = top + 0.1
    return top, bottom


def getAllSameScaleCode(code):
    top, bottom = getPercentile(code)
    cursor.execute("select 股票代码 from percentile where 排名 between " + str(top) +
                   " and " + str(bottom) + " except select " + str(code) + " order by 股票代码 desc;")
    rows = cursor.fetchall()
    codeList = []
    for row in rows:
        codeList.append(row[0])
    return codeList


def getSampleData(code, time):
    if code < 59999:
        cursor.execute("select (收盘价-前收盘) / 收盘价 涨跌幅, (最高价 - 最低价)/收盘价 振幅, (开盘价-前收盘)/前收盘 前振幅 "
                       "from daily_market_data_szse where 股票代码 = " + str(code) +
                       " order by 日期 desc limit " + str(time))
    else:
        cursor.execute("select (收盘价-前收盘) / 收盘价 涨跌幅, (最高价 - 最低价)/收盘价 振幅, (开盘价-前收盘)/前收盘 前振幅 "
                       "from daily_market_data_sse where 股票代码 = " + str(code) +
                       " order by 日期 desc limit " + str(time))
    rows = cursor.fetchall()
    return rows


# 计算两个向量的余弦相似度
def getDistance(data1, data2, length):
    ab, normA, normB = [0, 0, 0], [0, 0, 0], [0, 0, 0]
    for i in range(length):
        for j in range(0, 3):
            ab[j] += data1[i][j] * data2[i][j]
            normA[j] += data1[i][j] * data1[i][j]
            normB[j] += data2[i][j] * data2[i][j]
    result = 0.0
    coefficent = [0.8, 0.1, 0.1]
    for i in range(0, 3):
        if normA[i] * normB[i] == 0:
            return 0
        if (abs(float(ab[i])) / sqrt(normA[i] * normB[i])) > 1:
            result += 0
        else:
            result += (abs(float(ab[i])) / sqrt(normA[i] * normB[i])) * coefficent[i]
    return result


# code是被比对股票，time1是持续多少个交易日的K线，
# time2是最早追溯到的交易日, limit是相似度限制, 大于此限制才能被检出
# 检出的相似股票数据会被导入至similarStock表中
def getSimilarStock(code, time1, time2):
    print("正在分析与" + str(code) + "股票近" + str(time1) + "个交易日的走势图相近的K线")
    sampleData = getSampleData(code, time1)
    testList = getAllSameScaleCode(code)
    count = 0
    codeArray, dateArray, pointArray = [], [], []
    for testCode in testList:
        testData = getSampleData(testCode, time2)
        for i in range(20, len(testData) - len(sampleData)):
            dis = getDistance(testData[i:i + time1], sampleData, time1)
            # print(dis)
            if dis > 0.8:
                print(testCode, len(testData) - i, dis * 100)
                codeArray.append(testCode)
                dateArray.append(i)
                pointArray.append(dis * 100)
                count += 1
    print("共得到" + str(count) + "条结果")
    if count == 0:
        return -1
    pos, num = 0.0, 0.0
    for i in range(len(pointArray)):
        if pointArray[i] > num:
            pos = i
            num = pointArray[i]
    print("其中最相近的结果为" + str(codeArray[pos]) + "在" + str(dateArray[pos]) +
          "个交易日之前开始的走势，相似度评分为" + str(pointArray[pos]))
    print("请在SQL中执行下列语句")
    print("drop table if exists simstock1;")
    if code < 59999:
        print("create table simstock1 as select * from daily_market_data_szse where 股票代码 = " +
              str(code) + " order by 日期 desc limit " + str(time1) + ";")
    else:
        print("create table simstock1 as select * from daily_market_data_sse where 股票代码 = " +
              str(code) + " order by 日期 desc limit " + str(time1) + ";")
    print("drop table if exists simstock2;")
    if codeArray[pos] < 59999:
        print("create table simstock2 as select * from (select *, rank() "
                       "over(partition by 股票代码 order by 日期 desc) as r from daily_market_data_szse "
                       "dmds where 股票代码 = " + str(codeArray[pos]) + " )as t where t.r between " +
                       str(dateArray[pos] - time1 - 9) + " and " + str(dateArray[pos]) + ";")
    else:
        print("create table simstock2 as select * from (select *, rank() "
                       "over(partition by 股票代码 order by 日期 desc) as r from daily_market_data_sse "
                       "dmds where 股票代码 = " + str(codeArray[pos]) + " )as t where t.r between " +
                       str(dateArray[pos] - 9) + " and " + str(dateArray[pos] + time1) + ";")
    print("分析已经结束，请使用Tableau分析simStock1和simStock2两个股票")
    return 0


stockList = getAllSameScaleCode(601901)
for i in stockList:
    num = getSimilarStock(i, 30, 300)
    if num == 0:
        break;
cursor.close()
conn.close()
