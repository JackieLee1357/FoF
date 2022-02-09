

--//==========================Trace Result数据=====================================//--
--//原始数据，保留4小时
-- create table TraceResult(
--     id         String,
--     project    String,
--     process    String,
--     results    String,
--     createtime DateTime
-- )ENGINE = MergeTree
--     PARTITION BY (project, toDate(createtime))
--     PRIMARY KEY (project, process, createtime, id)
--     ORDER BY (project, process, createtime, id)
--     TTL createtime + toIntervalHour(4);


--//1天数据
create table TraceResult1Day(
    id         String,
    project    String,
    process    String,
    createtime DateTime,
    results    String
)ENGINE = MergeTree
    PARTITION BY (project, toDate(createtime))
    PRIMARY KEY (project, process, createtime, id)
    ORDER BY (project, process, createtime, id)
    TTL createtime + toIntervalDay(1),
    createtime + toIntervalDay(1) TO VOLUME 'default',
    createtime + toIntervalDay(1) TO DISK 'default';

CREATE MATERIALIZED VIEW view_TraceResult1Day to TraceResult1Day
AS SELECT id, project, process, createtime, results
FROM JGPWM.TraceResult
WHERE project = 'D16';


--//7天数据
-- drop table view_TraceResult1Week;
CREATE MATERIALIZED VIEW view_TraceResult1Week ENGINE = MergeTree()
     PARTITION BY (toDate(createtime))
    PRIMARY KEY (process, createtime, id)
    ORDER BY (process, createtime, id)
    TTL createtime + toIntervalDay(7),
    createtime + toIntervalDay(7) TO VOLUME 'default',
    createtime + toIntervalDay(7) TO DISK 'default'
AS SELECT *
FROM TraceResult1Day;


--//一个月数据
-- drop table view_TraceResult1Month;
CREATE MATERIALIZED VIEW view_TraceResult1Month ENGINE = MergeTree()
     PARTITION BY (toDate(createtime))
    PRIMARY KEY (process, createtime, id)
    ORDER BY (process, createtime, id)
    TTL createtime + toIntervalDay(30),
    createtime + toIntervalDay(30) TO VOLUME 'default',
    createtime + toIntervalDay(30) TO DISK 'default'
AS SELECT *
FROM TraceResult1Day;



--//6个月数据数据
-- drop table view_TraceResult6Months;
CREATE MATERIALIZED VIEW view_TraceResult6Months ENGINE = MergeTree()
     PARTITION BY (toDate(createtime))
    PRIMARY KEY (process, createtime, id)
    ORDER BY (process, createtime, id)
    TTL createtime + toIntervalDay(183),
    createtime + toIntervalDay(183) TO VOLUME 'default',
    createtime + toIntervalDay(183) TO DISK 'default'
POPULATE AS SELECT *
FROM view_TraceResult1Month WHERE createtime<addHours(now(), -716); --30天720小时，留4个小时的时间转移数据。



--//Bin值数据物化视图
drop table view_TraceResultBin;
CREATE MATERIALIZED VIEW view_TraceResultBin ENGINE = MergeTree()
     PARTITION BY (toDate(createtime))
    PRIMARY KEY (createtime,color, line,bin)
    ORDER BY (createtime,color, line,bin)
    TTL createtime + toIntervalDay(60),
    createtime + toIntervalDay(60) TO VOLUME 'default',
    createtime + toIntervalDay(60) TO DISK 'default'
POPULATE AS select  a.id
     ,b.sn
     ,b.lineid as line
     ,b.stationid as station
     ,b.RECIPE as color
     ,b.SIM_PROFILEID as bin
     ,b.logevent as event
    ,a.createtime
    ,visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test') as sub_test
,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'lower_limit')) as lower_limit
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'upper_limit')) as upper_limit
from TraceResult1Day a inner join TracePara1Day b on a.id = b.id
where a.process='ump-t1' and sub_test='413.L'
ORDER BY createtime DESC;

--//Bin值数据查询视图
CREATE VIEW view_TraceResultBinTmp as
select  line
     ,station
     ,color
     ,bin
     ,event
,round(avg(value),3) as value
,count(id) as qty
,toStartOfHour(createtime) as hour
from view_TraceResultBin
GROUP BY line,station, color, bin, toStartOfHour(createtime), event
ORDER BY hour DESC;

CREATE VIEW view_TraceResultBin as
select  a.id
     ,b.sn
     ,b.lineid as line
     ,b.stationid as station
     ,b.RECIPE as color
     ,b.SIM_PROFILEID as bin
     ,b.logevent as event
    ,a.createtime
    ,visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test') as sub_test
,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'lower_limit')) as lower_limit
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'upper_limit')) as upper_limit
from view_TraceResult1Month a inner join TracePara1Month b on a.id = b.id
where a.process='ump-t1' and sub_test='413.L'
ORDER BY createtime DESC;



CREATE MATERIALIZED VIEW view_TraceResultBin ENGINE = ReplacingMergeTree()
     PARTITION BY (toDate(createtime))
    PRIMARY KEY (createtime,color, line,bin)
    ORDER BY (createtime,color, line,bin, id)
    TTL createtime + toIntervalDay(60),
    createtime + toIntervalDay(60) TO VOLUME 'default',
    createtime + toIntervalDay(60) TO DISK 'default'
POPULATE AS select  a.id
     ,b.sn
     ,b.lineid as line
     ,b.stationid as station
     ,b.RECIPE as color
     ,b.SIM_PROFILEID as bin
     ,b.logevent as event
    ,a.createtime
    ,visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test') as sub_test
,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'lower_limit')) as lower_limit
-- ,avg(visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'upper_limit')) as upper_limit
from view_TraceResult1Month a inner join TracePara1Month b on a.id = b.id
where a.process='ump-t1' and sub_test='413.L'
ORDER BY createtime DESC;
--D49专案sub_test='413.L'为Bin值
--id='96cff1c4-7e48-11ec-aaaa-000bab45ceee'
--process='ump-t1';
-- FROM system.disks;
-- SELECT *
-- FROM system.storage_policies;

--//添加/修改定时删除任务TTL
-- alter table TraceParaAll modify ttl createtime + interval 2 week;
--
-- select addDays(now(), -7), addHours(now(), -167);
--
insert into view_TraceResultBin select  a.id
     ,b.sn
     ,b.lineid as line
     ,b.stationid as station
     ,b.RECIPE as color
     ,b.SIM_PROFILEID as bin
     ,b.logevent as event
    ,a.createtime
    ,visitParamExtractString(arrayJoin(JSONExtractArrayRaw(a.results)),'sub_test') as sub_test
,visitParamExtractFloat(arrayJoin(JSONExtractArrayRaw(a.results)),'value') as value
from view_TraceResult1Month a inner join TracePara1Month b on a.id = b.id
where a.process='ump-t1' and sub_test='413.L' and createtime>toStartOfHour(addHours(now(),-2))
ORDER BY createtime DESC;


