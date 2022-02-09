create table visits(userid int,visitdate date,website String) engine=MergeTree() PARTITION BY toYYYYMM(visitdate) order by userid;

CREATE TABLE visits
(
    `userid` int,
    `visitdate` date,
    `website` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(visitdate)
ORDER BY userid;



--插入数据:
insert into visits(userid,website,visitdate) values(100,'www.baidu.com','2020-06-01'),(100,'www.google.com','2020-07-02'),(100,'www.toutiao.com','2020-08-03');

INSERT INTO visits (userid, website, visitdate) VALUES();





select * from visits where visitdate='2020-07-02';

SELECT *
FROM visits
WHERE visitdate = '2020-07-02'


select database,table,partition,partition_id,name,path from system.parts where table='visits';

SELECT
    database,
    table,
    partition,
    partition_id,
    name,
    path
FROM system.parts
WHERE table = 'visits'



--1. 删除分区：
alter table visits drop partition 202007 ；

-- 重新插入数据：

 insert into visits(userid,website,visitdate) values(100,'www.baidu.com','2020-06-02')；

--可以实现数据更新，先删除分区再插入新的分区数据。

--2.复制分区数据：

Clickhouse> alter table visits replace partition 201908 from visits ;

ALTER TABLE visits
    REPLACE PARTITION 201908 FROM visits



--3. 重置分区：
Clickhouse> alter table visits CLEAR column website in partition 202007;

ALTER TABLE visits
    CLEAR COLUMN website     IN PARTITION 202007

--查询：
select * from visits FORMAT PrettyCompactMonoBlock;

SELECT *
FROM visits
FORMAT PrettyCompactMonoBlock


--4.卸载和装载分区：

alter table visits DETACH  partition 202007;

alter table visits ATTACH  partition 202007;

--5.分区备份

--6.分区还原：

--7.分区的删除：


--8.分区的移动
--9.分区的索引：
