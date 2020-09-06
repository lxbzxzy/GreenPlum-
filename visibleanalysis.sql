--第三部分：可视化分析部分

--服务器：腾讯云服务器，IP地址：49.234.213.42，GreenPlum端口号：5432

--1. 原始要求图片：为2020-06-24深交所上市公司总市值的地域和行业分布
--注意点：
--● 将日期拖入筛选器，以日为单位计算市值之和
--● 地图中有部分城市/地域无法自动识别，需要手动输入经纬度
--原始项目路径：visible analysis/历年沪深300行业和地域分布.twb
--原始项目路径：visible analysis/历年沪深300行业和地域分布视频.mp4(github暂不支持在线查看视频功能)

--2. 可视化任务一中得到的沪深300成分股总市值的地域和行业分布并制作动图展现分布情况随时间的变化。
--合并两市股票时需要将所属行业字段统一，
--将sh_stock_list中入选沪深300的股票的的行业并入sz_stock_list中的所属行业字段。
--为制作动图效果，将日期字段拖入页面，同样以日为单位。
--原始项目路径：visible analysis/深交所行业和地域分布.twb
--原始项目路径：visible analysis/深交所行业和地域分布.mp4(github暂不支持在线查看视频功能)

create table totalCSI300(原简称 varchar(32), 股票简称 varchar(32), 
	股票代码 numeric(6,0) not null, 省份 varchar(32), 所属行业 varchar(32), 
	公司全称 varchar(128), 指标 float, 市值 float,成交金额 float, 日期 date);
\copy totalcsi300 from '~/finalproject/totalcsi300.csv' (format 'csv', encoding 'utf8');

--对沪深300数据的提取和整理是采用python处理的
--详见csi300.sql


--为保护数据安全，数据库账户的密码不予显示，如有需要，可邮件咨询liu-xb18@mails.tsinghua.edu.cn
