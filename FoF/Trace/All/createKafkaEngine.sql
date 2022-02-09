--Result
CREATE TABLE ConsumerResult (
    json String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = '10.127.3.133:9092',
                            kafka_topic_list = 'traceMqResult',
                            kafka_group_name = 'test',
                            kafka_format = 'JSONAsString',
                            --kafka_row_delimiter = '\n',
                            kafka_num_consumers = 2;
--kafka_format = ['JSONAsString','JSONEachRow','JSONExtract','JSON']


create table TraceResult(
    id         String,
    project    String,
    process    String,
    results    String,
    createtime DateTime
) engine = MergeTree PARTITION BY (project,toDate(createtime))
        PRIMARY KEY (project, process,createtime, id)
        ORDER BY (project, process,createtime, id)
        SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW view_ConsumerResult TO TraceResult
    AS SELECT replaceAll(replaceAll(replaceAll(trimBoth(visitParamExtractRaw(json, 'id')),char(13),''),char(10),''),'"','') as id
            ,replaceAll(replaceAll(replaceAll(trimBoth(visitParamExtractRaw(json, 'project')),char(13),''),char(10),''),'"','') as project
            ,replaceAll(replaceAll(replaceAll(trimBoth(visitParamExtractRaw(json, 'process')),char(13),''),char(10),''),'"','') as process
            ,visitParamExtractRaw(json, 'results') as results
            ,now() as createtime
FROM ConsumerResult;


drop table TraceResult2;
drop table view_ConsumerResult2;
create table TraceResult2(
    id         String,
    project    String,
    process    String,
    results    String,
    createtime DateTime
) engine = MergeTree PARTITION BY (project,toDate(createtime))
        PRIMARY KEY (project, process,createtime, id)
        ORDER BY (project, process,createtime, id)
        SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW view_ConsumerResult2 TO TraceResult2
    AS SELECT trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'id'),'"','')) as id
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'project'),'"','')) as project
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'process'),'"','')) as process
            ,trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'results')) as results
            ,now() as createtime
FROM ConsumerResult;


--History
CREATE TABLE ConsumerHistory (
    json String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = '10.127.3.133:9092',
                            kafka_topic_list = 'traceMqHistory',
                            kafka_group_name = 'test',
                            kafka_format = 'JSONAsString',
                            --kafka_row_delimiter = '\n',
                            kafka_num_consumers = 2;
--kafka_format = ['JSONAsString','JSONEachRow','JSONExtract','JSON']

create table TraceHistory(
id String,
sn String,
sntype String,
agent String,
project String,
process String,
lineid String,
stationid String,
fixtureid String,
testresult String,
event String,
starttime DateTime,
endtime DateTime,
created DateTime,
createtime DateTime
)engine = MergeTree PRIMARY KEY (project,id)
ORDER BY (project,id)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW view_ConsumerHistory TO TraceHistory
    AS SELECT replaceAll(replaceAll(replaceAll(trimBoth(visitParamExtractRaw(json, 'id')),char(13),''),char(10),''),'"','') as id
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'sn'),'"','')) as sn
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'sntype'),'"','')) as sntype
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'agent'),'"','')) as agent
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'project'),'"','')) as project
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'process'),'"','')) as process
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'lineid'),'"','')) as lineid
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'stationid'),'"','')) as stationid
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'fixtureid'),'"','')) as fixtureid
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'testresult'),'"','')) as testresult
            ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'event'),'"','')) as event
            ,parseDateTimeBestEffortOrNull(trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'starttime'),'"',''))) as starttime
            ,parseDateTimeBestEffortOrNull(trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'endtime'),'"',''))) as endtime
            ,parseDateTimeBestEffortOrNull(trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'created'),'"',''))) as created
            ,now() as createtime
from ConsumerHistory;



--Log
CREATE TABLE ConsumerLog (
    json String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = '10.127.3.133:9092',
                            kafka_topic_list = 'traceMqlogs',
                            kafka_group_name = 'test',
                            kafka_format = 'JSONAsString',
                            --kafka_row_delimiter = '\n',
                            kafka_num_consumers = 2;
--kafka_format = ['JSONAsString','JSONEachRow','JSONExtract','JSON']

create table TraceLog(
id      String,
project String,
process String,
event    String,
fg   String,
band   String,
bg String,
sp String,
created DateTime,
defects String,
items String,
uut_attributes String,
test_attributes String,
test_station_attributes String,
createtime DateTime
)engine = MergeTree PRIMARY KEY (project,id)
ORDER BY (project,id)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW view_ConsumerLog TO TraceLog
    AS SELECT trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'id'),'"','')) as id
    ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'project'),'"','')) as project
    ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'process'),'"','')) as process
    ,trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'event'),'"','')) as event
    ,trimBoth(replaceAll(visitParamExtractRaw(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'serials'),'fg'),'"','')) as fg
    ,trimBoth(replaceAll(visitParamExtractRaw(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'serials'),'band'),'"','')) as band
    ,trimBoth(replaceAll(visitParamExtractRaw(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'serials'),'bg'),'"','')) as bg
    ,trimBoth(replaceAll(visitParamExtractRaw(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'serials'),'sp'),'"','')) as sp
    ,parseDateTimeBestEffortOrNull(trimBoth(replaceAll(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'created'),'"',''))) as created
    ,trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'defects')) as defects
    --,trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'data')) as data
    ,visitParamExtractRaw(trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'data')), 'items') as items
    ,visitParamExtractRaw(visitParamExtractRaw(trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'data')), 'insight'), 'uut_attributes') as uut_attributes
    ,visitParamExtractRaw(visitParamExtractRaw(trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'data')), 'insight'), 'test_attributes') as test_attributes
    ,visitParamExtractRaw(visitParamExtractRaw(trimBoth(visitParamExtractRaw(replaceAll(replaceAll(json,char(13),''),char(10),''), 'data')), 'insight'), 'test_station_attributes') as test_station_attributes
    ,now() as createtime
from ConsumerLog;


--//替换回车换行符
select replaceAll(replaceAll(json,char(13),''),char(10),'');

--编写TTL，定时删除数据
SELECT *
FROM system.disks;
SELECT *
FROM system.storage_policies;

create table t_table_ttl(id UInt64 comment '主键',create_time Datetime comment '创建时间',
product_desc String  comment '产品描述' TTL create_time + interval 10 minute,product_type UInt8 )
engine=MergeTree partition by toYYYYMM(create_time) order by create_time
TTL create_time  + INTERVAL 1 MONTH ,
    create_time + INTERVAL 1 WEEK TO VOLUME 'default',
    create_time + INTERVAL 2 WEEK TO DISK 'default';

--查看表的描述
DESC TraceResult;
DESCRIBE TraceResult;

alter table TraceResult modify ttl createtime + interval 2 month;











