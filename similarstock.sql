--第四部分：相似的股票K线

--服务器：腾讯云服务器，IP地址：49.234.213.42，GreenPlum端口号：5432

--理论分析：究竟能有几个指标
--对于单日来说：开盘价、最高价、最低价和收盘价是四个独立的变量
--对于股票相似性，单股价格并不重要（降低若干倍效果一样）
--因此只需要三个比例指标：怎么选？
--原始方法：收盘价？
--单日涨跌幅度大多在2%以内。比如一股一直为1，另一个在0.99和1.01波动
--计算得到余弦相似度为0.99995？？？太容易达到了？

--版本1：整体的涨跌幅走势（对两只股票全部交易日的涨跌幅求相关系数）
--● 目前时间是取两两重合的部分，但这样时间轴可能不均匀
--● 涨跌幅可能需要重新算一下，表中给的值里面有很多null

drop table if exists info;
create table info (code1 numeric(6), code2 numeric(6), correlation numeric(11, 10)); 

drop table if exists code;
create table code as select 股票代码 from daily_market_data_szse union select 股票代码 from daily_market_data_sse order by 股票代码;
-- 3927 altogether, from two markets

drop table if exists code_sz;
create table code_sz as select distinct(股票代码) from daily_market_data_szse;
select count(*) from code_sz;
-- 2237 altogether, from sz market

do $$
begin
    for i in 0..2235 loop 
        for j in i+1..2236 loop 
            drop table if exists tmp;
            create table tmp as select * from (
            (select 股票代码 code1, 日期 date1, 涨跌幅 idx1 
	from daily_market_data_szse 
	where 股票代码 in (
	    select 股票代码 from code_sz order by 股票代码 limit(1) offset(i)))
	as t1
	inner join 
	(select 股票代码 code2, 日期 date2, 涨跌幅 idx2 
	from daily_market_data_szse 
	where 股票代码 in (
	    select 股票代码 from code_sz
	    order by 股票代码 limit(1) offset(j))) as t2
	on t1.date1 = t2.date2);
	insert into info select * from (select distinct t.code1, t.code2, corr(idx1, idx2) correlation from 
	    (select * from tmp where idx1 notnull and idx2 notnull) as t group by t.code1, t.code2) as tb;
	    end loop;
	end loop;
end;
$$;

--版本1的测试：
select tb1.股票代码 code1, tb2.股票代码 code2, corr(tb1.涨跌幅, tb2.涨跌幅) correlation
    from sz100 tb1, sz100 tb2
    where tb1.股票代码 != tb2.股票代码
    and tb1.日期 = tb2.日期
    and tb1.日期 > date'2020-01-01'
    and tb2.日期 > date'2020-01-01'
    group by tb1.股票代码, tb2.股票代码
    order by correlation desc limit(1);

drop table if exists code1, code2;
create table code1 as select new_date 日期, 股票代码, 涨跌幅 from (select generate_series(date'2020-01-01', date'2020-06-24', interval'1 day') as new_date) m full join (select * from daily_market_data_szse where 股票代码=776 and 日期 > date'2020-01-01') as code1 on m.new_date = code1.日期;
create table code2 as select new_date 日期, 股票代码, 涨跌幅 from (select generate_series(date'2020-01-01', date'2020-06-24', interval'1 day') as new_date) m left join (select * from daily_market_data_szse where 股票代码=2736 and 日期 > date'2020-01-01') as code1 on m.new_date = code1.日期;

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
%matplotlib inline
df1 = pd.read_csv('code1.csv', encoding='utf8')
df2 = pd.read_csv('code2.csv', encoding='utf8')
df1 = df1.sort_values(by='日期')
df2 = df2.sort_values(by='日期')
fig = plt.figure(figsize=(25, 15))
ax = fig.add_subplot(111)
ax.plot(df1.日期, df1.涨跌幅)
ax.plot(df2.日期, df2.涨跌幅)

--版本2：局部的指标趋势
--目的：通过过去预测未来。
--模拟：假设今天是2020年6月24日，选中1只股票，搜索两年以来的历史数据，
--找到与该股票近30天之内相似的股票数据，并根据历史数据预测相关的走势。
--文件请见getsimilarstock.py
--理论结果可见/visible analysis/相似K线.png
--该理论结果为万科A（000002）和阳光城（000671）2020年6月24日之前三十个交易日的K线数据，相似度评分85.7%
--但是我们需要的是错开时间安排的结果，以获得之后的走势预测

--为保护数据安全，数据库账户的密码不予显示，如有需要，可邮件咨询liu-xb18@mails.tsinghua.edu.cn
