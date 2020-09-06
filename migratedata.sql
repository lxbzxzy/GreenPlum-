--第二部分：数据导入部分

--服务器：腾讯云服务器，IP地址：49.234.213.42，GreenPlum端口号：5432
--导入数据文件的方式：运用宝塔面板，将数据文件导入至~/finalproject/目录

--创建数据库并安装madlib

create database finalproject;
-- [gpadmin@VM-0-15-centos ~]$ /usr/local/greenplum-db/madlib/bin/madpack install  -s madlib -p greenplum -c gpadmin@VM-0-15-centos:5432/finalproject

--数据导入通过宝塔面板导入至

--创建数据库的相关表
create table sz_stock_list(板块 varchar(32) not null, 公司全称 varchar(128) not null, 英文名称 varchar(256), 注册地址 varchar(1024), A股代码 numeric(6,0) not null primary key, A股简称 varchar(32) not null, 上市日期 date not null, A股总股本 numeric(12,0), A股流通股本 numeric(12,0),  B股代码 numeric(6,0), B股简称 varchar(32), B股上市日期 date, B股总股本 numeric(12,0), B股流通股本 numeric(12,0), 地区 varchar(8) not null, 省份 varchar(32) not null, 城市 varchar(32) not null, 所属行业 varchar(128), 公司网址 varchar(256));
\copy sz_stock_list from '~/finalproject/szse_list.csv' (format 'csv', encoding 'utf8');
create table sh_stock_list(股票代码 numeric(6,0) not null primary key,股票简称 varchar(32) not null,省份 varchar(32),所属行业 varchar(128),公司全称 varchar(128) not null,英文名称 varchar(256),板块 varchar(32) not null,交易所 varchar(16),交易货币 varchar(16),股票状态 varchar(8),上市日期  date);
\copy sh_stock_list from '~/finalproject/sse_list.csv' (format 'csv', encoding 'utf8');

--原始数据表格需要进行清理：原始数据表格在股票代码这一格数字之前有个“ ' ”字符；需要把它清掉，然后数据库读取csv会把表头读出来，所以需要把表头删去；excel对空数据表示为none，需要把它转化为''。
--转化的代码请见：unionfiles.py文件

--中国股市的个股价格记录是贵州茅台（小数点前四位），所以用numeric(6, 2)表示较为合适，总市值最高也是贵州茅台（￥2trillion），应该用numeric(13, 0)来表示。
create table daily_market_data_sse(日期 date not null, 股票代码 numeric(6,0) not null, 股票简称 varchar(32) not null, 收盘价 numeric(6, 2), 最高价 numeric(6, 2), 最低价 numeric(6, 2), 开盘价 numeric(6, 2), 前收盘 numeric(6, 2), 涨跌额 numeric(6, 2), 涨跌幅 numeric(9, 4), 换手率 numeric(9, 4), 成交量 numeric(12, 0), 成交金额 numeric(13, 0), 总市值 numeric(13, 0), 流通市值 numeric(13, 0), 成交笔数 numeric(9,0), primary key(日期, 股票代码));
\copy daily_market_data_sse from '~/finalproject/totalsse.csv' (format 'csv', encoding 'GBK');
create table daily_market_data_szse(日期 date not null, 股票代码 numeric(6,0) not null, 股票简称 varchar(32) not null, 收盘价 numeric(6, 2), 最高价 numeric(6, 2), 最低价 numeric(6, 2), 开盘价 numeric(6, 2), 前收盘 numeric(6, 2), 涨跌额 numeric(6, 2), 涨跌幅 numeric(9, 4), 换手率 numeric(9, 4), 成交量 numeric(12, 0), 成交金额 numeric(13, 0), 总市值 numeric(13, 0), 流通市值 numeric(13, 0), 成交笔数 numeric(9,0), primary key(日期, 股票代码));
\copy daily_market_data_szse from '~/finalproject/totalszse.csv' (format 'csv', encoding 'GBK');

--数据库连接：Dbeavor, Tableau和Python都可以通过相同的账户连接
--其中python账户的连接方式可见https://www.cnblogs.com/flying-tiger/p/6704696.html这篇文章

--为保护数据安全，数据库账户的密码不予显示，如有需要，可邮件咨询liu-xb18@mails.tsinghua.edu.cn
