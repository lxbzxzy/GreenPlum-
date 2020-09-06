--第一部分：高级SQL

--服务器：腾讯云服务器，IP地址：49.234.213.42，GreenPlum端口号：5432

--数据加载的过程请见migratedata.sql文档

--首先对于成分股的变更，证监会变更日期通常为每年的1月和7月，样本选取时期为之前的半年。我们假定在1月变更的成分股参考的是前一年7月1日至12月31日的数据，7月变更的成分股参考的是前一年1月1日至6月30日的数据。

--选取相关时间段的SQL查询语句如下：
select case when date'查询日期' - (date_trunc('years', date '查询日期')::date + interval '6 month') > interval '0 days' 
then date_trunc('years', date '查询日期')::date 
else date_trunc('years', date '查询日期')::date - interval '6 month' end starttime,
case when date'查询日期' - (date_trunc('years', date '查询日期')::date + interval '6 month') > interval '0 days' 
then date_trunc('years', date '查询日期')::date + interval '6 month' - interval '1 hour' 
else date_trunc('years', date '查询日期')::date - interval '1 hour' end endtime;

--在查询中直接使用上述查询非常麻烦，将其封装为函数能提升效率：
create or replace function getStartTime(date)
returns timestamp
as
$$
    select case when  $1 - (date_trunc('years', $1)::date + interval '6 month') > interval '0 days' 
	then date_trunc('years',  $1)::date 
	else date_trunc('years',  $1)::date - interval '6 month' end
$$ LANGUAGE sql;
--getEndTime()的函数构造相同。

--之后，利用上面的确定起始终止日期的函数，确定深证100在指定日期的成分股信息。为了方便计算权重，我们计算每日的总流通市值和总成交金额作为视图，并把该视图join入select表
create view sz_daily_total as select 日期, sum(流通市值) 总流通市值, sum(成交金额) 总成交金额 from daily_market_data_szse dmds group by 日期;
create view sh_daily_total as select 日期, sum(流通市值) 总流通市值, sum(成交金额) 总成交金额 from daily_market_data_sse dmds group by 日期;
create view shsz_daily_total as select sh.日期, (sh.总成交金额+sz.总成交金额) 两市总成交金额, (sh.总流通市值 +sz.总流通市值 ) 两市总流通市值 from sh_daily_total sh, sz_daily_total sz where sh.日期=sz.日期;

--在查询语句中，日期是可以修改的变量，可对此创建SQL函数或Python函数

--***
--深证100成分股
--***
select a股简称, 股票代码, 地区, 所属行业, 公司全称 from (
	select 股票代码, (2 * sum(流通市值)/sum(总流通市值) + sum(成交金额)/sum(总成交金额)) / 3 指标 
		from daily_market_data_szse dmds, sz_daily_total sdt 
		where (dmds.日期 between getStartTime('2020-08-10') and getEndTime('2020-08-10')) 
			and dmds.日期 = sdt.日期 
		group by 股票代码 order by 指标 desc limit(100)) as t1
left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码 order by 股票代码;

--***
--沪深300成分股
--***
select * from (
select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_szse dmdsz, shsz_daily_total sdt
		where (dmdsz.日期 between getStartTime('2020-08-10') and getEndTime('2020-08-10')) 
			and dmdsz.日期 = sdt.日期
	group by 股票代码) as t1 left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码
union all
select 股票简称, t2.股票代码,省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_sse dmdsh, shsz_daily_total sdt
		where (dmdsh.日期 between getStartTime('2020-08-10') and getEndTime('2020-08-10')) 
			and dmdsh.日期 = sdt.日期
	group by 股票代码) as t2 left join sh_stock_list shl on t2.股票代码 = shl.股票代码) as ttotal
order by 指标 desc limit(300);

--中证100成分股在选取规则和沪深300的区别有两点，第一是在沪深300内部按照流通市值进行重新排序，第二是选取时间的不同，包括起始日期和持续时间。
--创建的新的时间选取方式如下。该选取方式简化为：以6月和12月为调整时期，持续时间为6月1日或12月1日开始的一年

create or replace function getStartTime2(date)
returns timestamp
as
$$
    select case when  $1 - (date_trunc('years', $1)::date + interval '5 month') > interval '0 days' 
    	and $1 - (date_trunc('years', $1)::date + interval '1 month') < interval '0 days'
	then date_trunc('years',  $1)::date - interval '7 month'
	when  $1 - (date_trunc('years', $1)::date + interval '5 month') < interval '0 days'
	then date_trunc('years',  $1)::date - interval '13 month'
	else date_trunc('years',  $1)::date - interval '1 month' end
$$ LANGUAGE sql;

--***
--中证100成分股
--***
select * from (
select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标2 from (
	select 股票代码, sum(流通市值)/sum(两市总流通市值) 指标2 
		from daily_market_data_szse dmdsz2, shsz_daily_total sdt2
		where (dmdsz2.日期 between getStartTime2('2020-08-10') and getEndTime2('2020-08-10')) 
			and dmdsz2.日期 = sdt2.日期
	group by 股票代码) as t12 left join sz_stock_list ssl2 on t12.股票代码 = ssl2.a股代码
union all
select 股票简称, t22.股票代码,省份, 所属行业, 公司全称, 指标2 from (
	select 股票代码, sum(流通市值)/sum(两市总流通市值) 指标2 
		from daily_market_data_sse dmdsh2, shsz_daily_total sdt2
		where (dmdsh2.日期 between getStartTime2('2020-08-10') and getEndTime2('2020-08-10')) 
			and dmdsh2.日期 = sdt2.日期
	group by 股票代码) as t22 left join sh_stock_list shl2 on t22.股票代码 = shl2.股票代码)as ttotal2
where 股票代码 in (select 股票代码 from (
select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_szse dmdsz, shsz_daily_total sdt
		where (dmdsz.日期 between getStartTime('2020-08-10') and getEndTime('2020-08-10')) 
			and dmdsz.日期 = sdt.日期
	group by 股票代码) as t1 left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码
union all
select 股票简称, t2.股票代码,省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_sse dmdsh, shsz_daily_total sdt
		where (dmdsh.日期 between getStartTime('2020-08-10') and getEndTime('2020-08-10')) 
			and dmdsh.日期 = sdt.日期
	group by 股票代码) as t2 left join sh_stock_list shl on t2.股票代码 = shl.股票代码) as ttotal
order by 指标 desc limit(300))
order by 指标2 desc limit(100);

--***
--中证200成分股
--***
--把中证100最后一行的desc limit(100)改为asc limit(200)

--优化：股票简称修改的情况：
--股票简称会发生更改，所以对于很久之前的股票，用现在的股票简称来表示会有所偏差，所以我们选取指标所取期间的最常用简称作为其股票简称的曾用名，为表示方便，约定以超过一半时间使用的简称作为原简称，如果没有简称满足上述条件，则原简称为null。修改后的深证100、沪深300、中证100、中证200查询语句如下：

--***
--深证100优化
--***
select sum2.股票简称 原简称, sum1.* from (select a股简称 现简称, 股票代码, 地区, 所属行业, 公司全称 from (
	select 股票代码, (2 * sum(流通市值)/sum(总流通市值) + sum(成交金额)/sum(总成交金额)) / 3 指标
		from daily_market_data_szse dmds, sz_daily_total sdt 
		where (dmds.日期 between getStartTime('2000-08-10') and getEndTime('2010-08-10')) 
			and dmds.日期 = sdt.日期 
		group by 股票代码 order by 指标 desc limit(100)) as t1
left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码 order by 股票代码) as sum1 
left join (select 股票代码, 股票简称 FROM daily_market_data_szse 
	WHERE 日期 BETWEEN getstarttime('2010-08-10') AND getendtime('2010-08-10')
    GROUP BY 股票代码,股票简称 having count(*) > 90) 
as sum2 on sum1.股票代码=sum2.股票代码 order by 股票代码;

--***
--沪深300优化
--***
select * from (
select sum2.股票简称 原简称, sum1.* from (select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_szse dmdsz, shsz_daily_total sdt
		where (dmdsz.日期 between getStartTime('2010-08-10') and getEndTime('2010-08-10')) 
			and dmdsz.日期 = sdt.日期
	group by 股票代码) as t1 left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码) as sum1 
left join (select 股票代码, 股票简称 FROM daily_market_data_szse 
	WHERE 日期 BETWEEN getstarttime('2010-08-10') AND getendtime('2010-08-10')
    GROUP BY 股票代码,股票简称 having count(*) > 90) 
as sum2 on sum1.股票代码=sum2.股票代码
union all
select sum4.股票简称 原简称, sum3.* from (select 股票简称, t2.股票代码,省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_sse dmdsh, shsz_daily_total sdt
		where (dmdsh.日期 between getStartTime('2010-08-10') and getEndTime('2010-08-10')) 
			and dmdsh.日期 = sdt.日期
	group by 股票代码) as t2 left join sh_stock_list shl on t2.股票代码 = shl.股票代码) as sum3
left join (select 股票代码, 股票简称 FROM daily_market_data_sse 
	WHERE 日期 BETWEEN getstarttime('2010-08-10') AND getendtime('2010-08-10')
    GROUP BY 股票代码,股票简称 having count(*) > 90) 
as sum4 on sum3.股票代码=sum4.股票代码) as total
order by total.指标 desc limit(300);

--***
--中证100
--***
select * from (
select sum2.股票简称 原简称, sum1.* from (select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标2 from (
	select 股票代码, sum(流通市值)/sum(两市总流通市值) 指标2 
		from daily_market_data_szse dmdsz2, shsz_daily_total sdt2
		where (dmdsz2.日期 between getStartTime2('2010-08-10') and getEndTime2('2010-08-10')) 
			and dmdsz2.日期 = sdt2.日期
	group by 股票代码) as t12 left join sz_stock_list ssl2 on t12.股票代码 = ssl2.a股代码) as sum1
	left join (select 股票代码, 股票简称 FROM daily_market_data_szse 
		WHERE 日期 BETWEEN getstarttime2('2010-08-10') AND getendtime2('2010-08-10')
    	GROUP BY 股票代码,股票简称 having count(*) > 180) 
	as sum2 on sum1.股票代码=sum2.股票代码
union all
select sum4.股票简称 原简称, sum3.* from (select 股票简称, t22.股票代码,省份, 所属行业, 公司全称, 指标2 from (
	select 股票代码, sum(流通市值)/sum(两市总流通市值) 指标2 
		from daily_market_data_sse dmdsh2, shsz_daily_total sdt2
		where (dmdsh2.日期 between getStartTime2('2010-08-10') and getEndTime2('2010-08-10')) 
			and dmdsh2.日期 = sdt2.日期
	group by 股票代码) as t22 left join sh_stock_list shl2 on t22.股票代码 = shl2.股票代码) as sum3
	left join (select 股票代码, 股票简称 FROM daily_market_data_sse 
		WHERE 日期 BETWEEN getstarttime2('2010-08-10') AND getendtime2('2010-08-10')
    	GROUP BY 股票代码,股票简称 having count(*) > 180) 
	as sum4 on sum3.股票代码=sum4.股票代码) as totalsum
where 股票代码 in (select 股票代码 from (
select a股简称 股票简称, 股票代码, 省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_szse dmdsz, shsz_daily_total sdt
		where (dmdsz.日期 between getStartTime('2010-08-10') and getEndTime('2010-08-10')) 
			and dmdsz.日期 = sdt.日期
	group by 股票代码) as t1 left join sz_stock_list ssl on t1.股票代码 = ssl.a股代码
union all
select 股票简称, t2.股票代码,省份, 所属行业, 公司全称, 指标 from (
	select 股票代码, (2 * sum(流通市值)/sum(两市总流通市值) + sum(成交金额)/sum(两市总成交金额))/3 指标 
		from daily_market_data_sse dmdsh, shsz_daily_total sdt
		where (dmdsh.日期 between getStartTime('2010-08-10') and getEndTime('2010-08-10')) 
			and dmdsh.日期 = sdt.日期
	group by 股票代码) as t2 left join sh_stock_list shl on t2.股票代码 = shl.股票代码) as ttotal
order by 指标 desc limit(300))
order by 指标2 desc limit(100);

--***
--中证200
--***
--把中证100最后一行的desc limit(100)改为asc limit(200)


--为保护数据安全，数据库账户的密码不予显示，如有需要，可邮件咨询liu-xb18@mails.tsinghua.edu.cn
