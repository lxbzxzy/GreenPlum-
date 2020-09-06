import csv
# 导入psycopg2包
import psycopg2


def getCSI300(str):
    return "select *,"+str+" from (select sum2.股票简称 原简称, sum1.* from (select a股简称 股票简称, 股票代码, 省份, 所属行业, " \
    "公司全称, 指标,市值,成交金额 from (select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 " \
    "指标, avg(流通市值) 市值, avg(成交金额) 成交金额 from " \
    "daily_market_data_szse dmdsz, shsz_daily_total sdt where (dmdsz.日期 between getStartTime("+str+") and " \
    "getEndTime("+str+")) and dmdsz.日期 = sdt.日期 group by 股票代码) as t1 left join sz_stock_list ssl on t1.股票代码 = " \
    "ssl.a股代码) as sum1 left join (select 股票代码, 股票简称 FROM daily_market_data_szse WHERE 日期 BETWEEN " \
    "getstarttime("+str+") AND getendtime("+str+") GROUP BY 股票代码,股票简称 having count(*) > 90) as sum2 on " \
    "sum1.股票代码=sum2.股票代码 union all select sum4.股票简称 原简称, sum3.* from (select 股票简称, t2.股票代码,省份, " \
    "所属行业, 公司全称, 指标, 市值, 成交金额 from (select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/" \
    "sum(两市总成交金额))/3 指标, avg(流通市值) 市值, avg(成交金额) 成交金额 from daily_market_data_sse dmdsh, " \
    "shsz_daily_total sdt where (dmdsh.日期 between getStartTime('2010-08-10') and getEndTime('2010-08-10')) and " \
    "dmdsh.日期 = sdt.日期 group by 股票代码) as t2 left join sh_stock_list shl on t2.股票代码 = shl.股票代码) as sum3 " \
    "left join (select 股票代码, 股票简称 FROM daily_market_data_sse WHERE 日期 BETWEEN getstarttime('2010-08-10') AND " \
    "getendtime('2010-08-10') GROUP BY 股票代码,股票简称 having count(*) > 90) as sum4 on sum3.股票代码=sum4.股票代码) as " \
    "total order by total.指标 desc limit(300);"


fwrite = open('../totalcsi300.csv', 'w', encoding='utf-8', newline='')
csv_writer = csv.writer(fwrite)
# 连接到一个给定的数据库
conn = psycopg2.connect(database="finalproject", user="usr",
                        password="*******hidden password*******", host="49.234.213.42", port="5432")
# 建立游标，用来执行数据库操作
cursor = conn.cursor()

# 提交SQL命令
conn.commit()

timeArray = ["\'2005-1-10\'", "\'2005-7-10\'", "\'2006-1-10\'", "\'2006-7-10\'",
             "\'2007-1-10\'", "\'2007-7-10\'", "\'2008-1-10\'", "\'2008-7-10\'",
             "\'2009-1-10\'", "\'2009-7-10\'", "\'2010-1-10\'", "\'2010-7-10\'",
             "\'2011-1-10\'", "\'2011-7-10\'", "\'2012-1-10\'", "\'2012-7-10\'",
             "\'2013-1-10\'", "\'2013-7-10\'", "\'2014-1-10\'", "\'2014-7-10\'",
             "\'2015-1-10\'", "\'2015-7-10\'", "\'2016-1-10\'", "\'2016-7-10\'",
             "\'2017-1-10\'", "\'2017-7-10\'", "\'2018-1-10\'", "\'2018-7-10\'",
             "\'2019-1-10\'", "\'2019-7-10\'", "\'2020-1-10\'", "\'2020-7-10\'",
             ]

# 获取SELECT返回的元组
for time in timeArray:
    cursor.execute(getCSI300(time))
    rows = cursor.fetchall()
    for row in rows:
        csv_writer.writerow(row)
        print(row)

# 关闭游标
cursor.close()

# 关闭数据库连接
conn.close()
