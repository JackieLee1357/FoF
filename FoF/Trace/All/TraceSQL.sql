SELECT distinct a.*, b.sn, b.sntype, b.agent, b.lineid,
 b.stationid, b.STATION_STRING, b.color, b.Build,
 b.fixtureid, b.testresult, b.starttime, b.endtime
FROM JGPWMD49.TraceResultAll a
 left join TraceParaAll b on a.id = b.id
WHERE process = 'ump-t1' and result <> 'pass';   --查询Fail数据

SELECT distinct b.sn, b.sntype, b.agent, b.lineid,
 b.stationid, b.STATION_STRING, b.color, b.Build,
 b.fixtureid, b.testresult, b.starttime, b.endtime
FROM JGPWMD49.view_TraceResultFailedUmpT1 a
 left join TraceParaAll b on a.sn = b.sn
WHERE  process <> 'ump-t1' and result <> 'pass' ;   --根据Fail数据查询SN过站信息


SELECT distinct sn, sntype, agent, lineid,
 stationid, fixtureid, logevent, testresult, starttime, endtime
FROM JGPWM.view_TraceMQParameter
WHERE logevent == 'fail' or logevent == 'scrap' or testresult == 'fail' ;   --根据Fail数据查询SN过站信息

SELECT distinct sn, sntype
FROM JGPWM.view_TraceMQParameter
WHERE logevent == 'fail' or logevent == 'scrap' or testresult == 'fail' ;   --根据Fail数据查询SN过站信息

select distinct length(sn)
, sn, sntype
from JGPWM.view_TraceMQParameter
where length(sn)=71;

SELECT project , process , sn , sntype , agent, testresult , lineid , starttime, endtime , fixtureid , logcreated,
 hiscreated, logevent , stationid , defects , uut_stop, uut_start, test_result , unit_serial_number , line_id ,
 fixture_id , station_id , software_name , software_version
FROM JGPWMD49.TraceParaAll a inner join (SELECT distinct band,bg,fg,sp FROM JGPWMD49.TraceParaAll
WHERE (logevent == 'fail' or logevent == 'scrap' or testresult == 'fail') and endtime>toStartOfHour(addHours(now(), -1))) b
    on (a.sn=b.band or a.sn=b.bg or a.sn=b.sp or a.sn=b.fg);

select toStartOfHour(addHours(now(), -1));   --前一个小时


select addHours(now(), -1);


SELECT distinct process
from JGPWM.TraceMQResult
WHERE project='D49'
ORDER BY process;

select toStartOfHour(addHours(now(), -1));

select (addMinutes(now(), -30));

SELECT *
            FROM JGPWM.view_TraceMQParameter WHERE process='a-shear' and project='D49'
            AND endtime>toStartOfHour(addHours(now(), -1));

select distinct id from JGPWMD49.TraceParaAll;


SELECT id,
       project,
       process,
       sn,
       sntype,
       agent,
       testresult,
       lineid,
       starttime,
       endtime,
       fixtureid,
       a.created AS logcreated,
       b.created AS hiscreated,
       a.event   AS logevent,
       b.event   AS hisevent,
       stationid,
       serials,
       defects,
       data
FROM JGPWM.TraceMQLog AS a
INNER JOIN JGPWM.TraceMQHistory AS b ON a.id = b.id
WHERE endtime>toStartOfHour(addHours(now(), {hour}))
and process='{pcs}' and project='{pjt}';


CREATE VIEW JGPWM.view_TraceMQResult AS
SELECT *
FROM JGPWM.TraceMQResult a inner join JGPWM.TraceMQHistory b
on a.id=b.id
where starttime>toStartOfHour(addHours(now(), -1));


SELECT id,
           project,
           process,
           agent,
           created AS logcreated,
           event   AS logevent,
           serials,
           defects,
           data
            FROM JGPWM.TraceMQLog
            WHERE TraceMQLog.created>toStartOfHour(addHours(now(), {hour}))
            and process='{pcs}' and project='{pjt}';


SELECT id,
           project,
           process,
           sn,
           sntype,
           agent,
           testresult,
           lineid,
           starttime,
           endtime,
           fixtureid,
           created AS hiscreated,
           event   AS hisevent,
           stationid
            FROM JGPWM.TraceMQHistory
            WHERE endtime>toStartOfHour(addHours(now(), {hour}))
            and process='{pcs}' and project='{pjt}';



select toStartOfHour(addHours(now(), -6));


alter table JGPWMD79.TraceParaAll delete where project<>'D79';


alter table JGPWM.TraceMQHistory delete where created<toStartOfHour(addHours(now(), -6));

SELECT id,
           project,
           process,
           agent,
           created AS logcreated,
           event   AS logevent,
           serials,
           defects,
           data
            FROM JGPWM.TraceMQLog;


SELECT id,
           project,
           process,
           sn,
           sntype,
           agent,
           testresult,
           lineid,
           a.createtime,
           starttime,
           endtime,
           fixtureid,
           a.created AS logcreated,
           b.created AS hiscreated,
           a.event   AS logevent,
           b.event   AS hisevent,
           stationid,
           serials,
           defects,
           data
            FROM JGPWMD49.TraceMQLog AS a
            INNER JOIN JGPWMD49.TraceMQHistory AS b ON a.id = b.id
            prewhere createtime>toDateTime('2022-01-12 13:00:00');


SELECT id, results, project, process, createtime
FROM JGPWMD49.TraceMQResult prewhere createtime>toDateTime('2022-01-12 13:00:00');

SELECT id, sn , project , process , sntype , agent, testresult , lineid , createtime, starttime, endtime , fixtureid , logcreated,
        hiscreated, logevent , stationid , defects , uut_stop, uut_start, test_result , unit_serial_number , line_id ,
        fixture_id , station_id , software_name , software_version
        FROM JGPWMD49.TraceParaAll a inner join (SELECT distinct band,bg,fg,sp FROM JGPWMD49.TraceParaAll
        prewhere (logevent == 'fail' or logevent == 'scrap' or testresult == 'fail')
        AND createtime>toDateTime('2022-01-12 13:00:00')) b
        on (a.sn=b.band or a.sn=b.bg or a.sn=b.sp or a.sn=b.fg);

SELECT id, results, project, process, createtime
FROM JGPWMD49.TraceMQResult prewhere id in
(select id as id from JGPWMD49.TraceFailed prewhere createtime>toDateTime('2022-01-12 11:00:00'));


SELECT id, results, project, process, createtime FROM JGPWMD63.TraceMQResult prewhere id in('ad7cad46-74e0-11ec-861c-00073271ad80','50bcc7d6-74df-11ec-bf34-0007326cb8f2','0463a2e4-74e1-11ec-bf34-0007326cb8f2','ad9b8a35-74e0-11ec-a7ee-00073271b134','835a80d1-74e1-11ec-8b27-00073271afce','9ae0878d-74e1-11ec-a7ee-00073271b134','3b31f257-74e2-11ec-b7ff-00073271af0c','768103fe-74e2-11ec-b7ff-00073271af0c','324dc0d2-74e2-11ec-898d-00073271b1ce','641423b8-74e0-11ec-bf34-0007326cb8f2','d1487dc6-74e0-11ec-a286-00073256e520','b32b50fb-74e2-11ec-a7ee-00073271b134','916aba30-74df-11ec-898d-00073271b1ce','b18d5d23-74e0-11ec-bf34-0007326cb8f2','1faf9cfb-74e1-11ec-a7ee-00073271b134','ca2f2e02-74e0-11ec-a7ee-00073271b134','95118b51-74e2-11ec-a723-5ebb1c4c01c1','95169c33-74e2-11ec-9c31-0a62e3e234c4','95169c33-74e2-11ec-a54c-c6d4569f4536');


SELECT id, project, process, sn, sntype, agent, testresult, lineid, a.createtime, starttime,
    endtime, fixtureid, a.created AS logcreated, b.created AS hiscreated, a.event AS logevent, b.event AS hisevent,
    stationid, serials, defects, data FROM JGPWMD49.TraceMQLog a INNER JOIN JGPWMD49.TraceMQHistory b ON a.id = b.id
    prewhere a.createtime>toDateTime('2022-01-18 20:27:48');

SELECT id, project, process, agent,a.createtime,
    a.created AS logcreated,  a.event AS logevent,
    serials, defects, data FROM JGPWMD49.TraceMQLog a
    prewhere a.createtime>toDateTime('2022-01-18 20:27:48');
