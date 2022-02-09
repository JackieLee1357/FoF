

--//==========================Trace Para数据=====================================//--
--//原始数据，分两个表：TraceLog/TraceHistory,处理完立即删除，默认保留两天数据。
create table TraceHistory(
    id         String,
    sn         String,
    sntype     String,
    agent      String,
    project    String,
    process    String,
    lineid     String,
    stationid  String,
    fixtureid  String,
    testresult String,
    event      String,
    starttime  DateTime,
    endtime    DateTime,
    created    DateTime,
    createtime DateTime default now()) ENGINE = MergeTree
        PRIMARY KEY (project, id)
        ORDER BY (project, id)
        TTL createtime + toIntervalDay(2);

create table TraceLog(
    id                      String,
    project                 String,
    process                 String,
    event                   String,
    fg                      String,
    band                    String,
    bg                      String,
    sp                      String,
    created                 DateTime,
    defects                 String,
    items                   String,
    uut_attributes          String,
    test_attributes         String,
    test_station_attributes String,
    createtime              DateTime default now())ENGINE = MergeTree
        PRIMARY KEY (project, id)
        ORDER BY (project, id)
        TTL createtime + toIntervalDay(2);


--//1天数据，原始数据由python处理，合成一个大宽表，保留1天数据。
create table TracePara1Day(
    id                                String,
    sn                                String,
    sntype                            String,
    agent                             String,
    process                           String,
    lineid                            String,
    stationid                         String,
    testresult                        String,
    createtime                        DateTime,
    starttime                         DateTime,
    endtime                           DateTime,
    fixtureid                         String,
    logcreated                        DateTime,
    hiscreated                        DateTime,
    logevent                          String,
    hisevent                          String,
    defects                           String,
    band                              String,
    sp                                String,
    bg                                String,
    fg                                String,
    STATION_STRING                    String,
    uut_stop                          String,
    uut_start                         String,
    test_result                       String,
    unit_serial_number                String,
    line_id                           String,
    fixture_id                        String,
    station_id                        String,
    software_name                     String,
    software_version                  String
        ) ENGINE = MergeTree
        PARTITION BY toDate(createtime)
        PRIMARY KEY (process,lineid,stationid, createtime, id, sn)
        ORDER BY (process,lineid,stationid, createtime, id, sn)
        TTL createtime  + INTERVAL 1 day,
        createtime + INTERVAL 1 day TO VOLUME 'default',
        createtime + INTERVAL 1 day TO DISK 'default';



--//7天数据
CREATE MATERIALIZED VIEW view_TracePara1Week ENGINE = MergeTree()
    PARTITION BY toDate(createtime)
    PRIMARY KEY (process,lineid,stationid, createtime, id, sn)
    ORDER BY (process,lineid,stationid, createtime, id, sn)
    TTL createtime  + INTERVAL 7 day,
    createtime + INTERVAL 7 day TO VOLUME 'default',
    createtime + INTERVAL 7 day TO DISK 'default'
AS SELECT *
FROM TracePara1Day;


--//一个月数据
-- drop table view_TracePara1Week;
-- drop table view_TracePara1Month;
-- drop table view_TracePara6Months;
CREATE MATERIALIZED VIEW view_TracePara1Month to TracePara1Month
AS SELECT *
FROM TracePara1Day;


-- alter table TracePara1Month modify ttl createtime + interval 30 day;
-- alter table TracePara6Months modify ttl createtime + interval 183 day;

--//6个月数据数据
-- drop table view_TracePara6Months;
CREATE MATERIALIZED VIEW view_TracePara6Months ENGINE = MergeTree()
    PARTITION BY toDate(createtime)
    PRIMARY KEY (process,lineid,stationid, createtime, id, sn)
    ORDER BY (process,lineid,stationid, createtime, id, sn)
    TTL createtime  + INTERVAL 183 day,
    createtime + INTERVAL 183 day TO VOLUME 'default',
    createtime + INTERVAL 183 day TO DISK 'default'
    POPULATE
AS SELECT *
FROM view_TracePara1Month WHERE createtime<addHours(now(), -716); --30天720小时，留4个小时的时间转移数据。

CREATE MATERIALIZED VIEW view_TracePara6Months to TracePara6Months
AS SELECT *
FROM TracePara1Month WHERE createtime<addHours(now(), -716); --30天720小时，留4个小时的时间转移数据。






--select database,table,partition,partition_id,name,path from system.parts where database='JGPWMD49' and table='TraceMQHistory';

-- SELECT *
-- FROM system.disks;
-- SELECT *
-- FROM system.storage_policies;

--//添加/修改定时删除任务TTL
-- alter table TraceParaAll modify ttl createtime + interval 2 week;
--
--
-- select addDays(now(), -7), addHours(now(), -167);
--

