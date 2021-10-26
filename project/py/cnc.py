import pymssql #引入pymssql模块

connect = pymssql.connect('CNWXIM0TRSQLV4A', 'PBIuser', 'PBIUser123456', 'SMDP') #服务器名,账户,密码,数据库名
if connect:
    print("连接成功!")
    
cursor = connect.cursor()   #创建一个游标对象,python里的sql语句都要通过cursor来执行
sql= """
--每天定时执行，插入数据到Temp表
DECLARE @INDEX AS INT
DECLARE @INDEX2 AS INT
DECLARE @TIME AS INT
DECLARE @TIMEMAX AS INT
SET @INDEX =1
SET @TIME = -1
SET @TIMEMAX = 1
CREATE TABLE #TABLE1( 
            KEYWORD  nvarchar(150)
			,MachineName nvarchar(50)
			,EventTime datetime	
			,CreateTime datetime
			 ,[Macro934] nvarchar(50)
      ,[Macro936] nvarchar(50)
      ,[Macro596] nvarchar(50)
      ,[Macro595] nvarchar(50)
      ,[Macro709] nvarchar(50)
      ,[Macro712] nvarchar(50)
      ,[Macro609] nvarchar(50)
      ,[Macro612] nvarchar(50)
      ,[Macro933] nvarchar(50)
      ,[Macro667] nvarchar(50)
      ,[Macro666] nvarchar(50)
      ,[Macro827] nvarchar(50)
      ,[Macro682] nvarchar(50)
      ,[Macro691] nvarchar(50)
      ,[Macro652] nvarchar(50)
      ,[Macro653] nvarchar(50)
      ,[Macro654] nvarchar(50)
      ,[Macro655] nvarchar(50)
      ,[Macro656] nvarchar(50)
      ,[Macro657] nvarchar(50)
      ,[Macro658] nvarchar(50)
      ,[Macro659] nvarchar(50)
      ,[Macro660] nvarchar(50)
      ,[Macro661] nvarchar(50)
      ,[Macro633] nvarchar(50)
      ,[Macro634] nvarchar(50)
      ,[Macro635] nvarchar(50)
      ,[Macro627] nvarchar(50)
      ,[Macro628] nvarchar(50)
      ,[Macro623] nvarchar(50)
      ,[Macro624] nvarchar(50)
      ,[Macro625] nvarchar(50)
      ,[Macro626] nvarchar(50)

			)
CREATE TABLE #TABLE2(ROWID INT,KEYWORD NVARCHAR(150))
CREATE TABLE #TABLE3( ROW_INDEX INT
		    ,KEYWORD  nvarchar(150)
			,MachineName nvarchar(50)
			,EventTime datetime					   
			 ,[Macro934] nvarchar(50)
      ,[Macro936] nvarchar(50)
      ,[Macro596] nvarchar(50)
      ,[Macro595] nvarchar(50)
      ,[Macro709] nvarchar(50)
      ,[Macro712] nvarchar(50)
      ,[Macro609] nvarchar(50)
      ,[Macro612] nvarchar(50)
      ,[Macro933] nvarchar(50)
      ,[Macro667] nvarchar(50)
      ,[Macro666] nvarchar(50)
      ,[Macro827] nvarchar(50)
      ,[Macro682] nvarchar(50)
      ,[Macro691] nvarchar(50)
      ,[Macro652] nvarchar(50)
      ,[Macro653] nvarchar(50)
      ,[Macro654] nvarchar(50)
      ,[Macro655] nvarchar(50)
      ,[Macro656] nvarchar(50)
      ,[Macro657] nvarchar(50)
      ,[Macro658] nvarchar(50)
      ,[Macro659] nvarchar(50)
      ,[Macro660] nvarchar(50)
      ,[Macro661] nvarchar(50)
      ,[Macro633] nvarchar(50)
      ,[Macro634] nvarchar(50)
      ,[Macro635] nvarchar(50)
      ,[Macro627] nvarchar(50)
      ,[Macro628] nvarchar(50)
      ,[Macro623] nvarchar(50)
      ,[Macro624] nvarchar(50)
      ,[Macro625] nvarchar(50)
      ,[Macro626] nvarchar(50)
  )
INSERT INTO #TABLE2  
	SELECT ROW_NUMBER() OVER(ORDER BY KEYWORD) AS ROWID
				,KEYWORD
		     FROM  (SELECT DISTINCT 
			      [EMT]+[Macro652]+[Macro653]+[Macro654]+[Macro655]+[Macro656]+[Macro657]+[Macro658]+[Macro659]+[Macro660]+[Macro661]+[Macro633]+[Macro634]+[Macro635]+[Macro627]+[Macro691] AS KEYWORD    
			    FROM [SMDP].[dbo].[ProbeValue] 
			  WHERE EventTime> DATEADD(DAY,@TIME, GETDATE()) AND EventTime< DATEADD(DAY,@TIMEMAX, GETDATE()) AND EMT IN(SELECT [EMT] FROM [SMDP].[dbo].[viewMachineList] WHERE Station LIKE 'WXI_Metal_%') )A 
INSERT INTO #TABLE3 SELECT 
	  ROW_NUMBER() OVER(  ORDER BY MachineName, EventTime )  AS ROW_INDEX
	  ,A.[EMT]+[Macro652]+[Macro653]+[Macro654]+[Macro655]+[Macro656]+[Macro657]+[Macro658]+[Macro659]+[Macro660]+[Macro661]+[Macro633]+[Macro634]+[Macro635]+[Macro627]+[Macro691] AS KEYWORD
      ,[MachineName]
	  ,[EventTime]
      ,[Macro934]
      ,[Macro936]
      ,[Macro596]
      ,[Macro595]
      ,[Macro709]
      ,[Macro712]
      ,[Macro609]
      ,[Macro612]
      ,[Macro933]
      ,[Macro667]
      ,[Macro666]
      ,[Macro827]
      ,[Macro682]
      ,[Macro691]
      ,[Macro652]
      ,[Macro653]
      ,[Macro654]
      ,[Macro655]
      ,[Macro656]
      ,[Macro657]
      ,[Macro658]
      ,[Macro659]
      ,[Macro660]
      ,[Macro661]
      ,[Macro633]
      ,[Macro634]
      ,[Macro635]
      ,[Macro627]
      ,[Macro628]
      ,[Macro623]
      ,[Macro624]
      ,[Macro625]
      ,[Macro626]
	 FROM [SMDP].[dbo].[ProbeValue] A INNER JOIN (SELECT [EMT] FROM [SMDP].[dbo].[viewMachineList] WHERE Station LIKE 'WXI_Metal_%') B ON A.EMT=B.EMT
	 WHERE EventTime> DATEADD(DAY,@TIME, GETDATE()) AND EventTime< DATEADD(DAY,@TIMEMAX, GETDATE()) 

SET @INDEX2 =(SELECT COUNT(KEYWORD) FROM #TABLE2)

WHILE @INDEX<=@INDEX2
BEGIN
INSERT INTO #TABLE1 
SELECT  KEYWORD
	  ,[MachineName]
	  ,EventTime
	  ,DATEADD(SS,-FLOOR(RAND()*10000),EventTime) AS CreateTime
	  ,[Macro934]
      ,[Macro936]
      ,[Macro596]
      ,[Macro595]
      ,[Macro709]
      ,[Macro712]
      ,[Macro609]
      ,[Macro612]
      ,[Macro933]
      ,[Macro667]
      ,[Macro666]
      ,[Macro827]
      ,[Macro682]
      ,[Macro691]
      ,[Macro652]
      ,[Macro653]
      ,[Macro654]
      ,[Macro655]
      ,[Macro656]
      ,[Macro657]
      ,[Macro658]
      ,[Macro659]
      ,[Macro660]
      ,[Macro661]
      ,[Macro633]
      ,[Macro634]
      ,[Macro635]
      ,[Macro627]
      ,[Macro628]
      ,[Macro623]
      ,[Macro624]
      ,[Macro625]
      ,[Macro626]

FROM 
  (SELECT ROW_NUMBER() OVER(ORDER BY MachineName, EventTime) AS ROW_INDEX1
      ,KEYWORD
	  ,[MachineName]
	  ,EventTime
	  ,[Macro934]
      ,[Macro936]
      ,[Macro596]
      ,[Macro595]
      ,[Macro709]
      ,[Macro712]
      ,[Macro609]
      ,[Macro612]
      ,[Macro933]
      ,[Macro667]
      ,[Macro666]
      ,[Macro827]
      ,[Macro682]
      ,[Macro691]
      ,[Macro652]
      ,[Macro653]
      ,[Macro654]
      ,[Macro655]
      ,[Macro656]
      ,[Macro657]
      ,[Macro658]
      ,[Macro659]
      ,[Macro660]
      ,[Macro661]
      ,[Macro633]
      ,[Macro634]
      ,[Macro635]
      ,[Macro627]
      ,[Macro628]
      ,[Macro623]
      ,[Macro624]
      ,[Macro625]
      ,[Macro626]
   FROM  #TABLE3
   WHERE KEYWORD = (SELECT KEYWORD
	                FROM  #TABLE2 
	                WHERE ROWID=@INDEX) ) D
WHERE D.ROW_INDEX1=1
SET @INDEX=@INDEX+1
END

IF NOT EXISTS(
SELECT B.EventTime 
FROM [SMDP].[dbo].[ProbeValueTemp] A INNER JOIN #TABLE1 B
ON  A.CreateTime=B.EventTime )
BEGIN
INSERT INTO [SMDP].[dbo].[ProbeValueTemp] 
SELECT 
	  ''
	  ,[MachineName]
	  ,''
	  ,''
	  ,''
	  ,''
	  ,CreateTime
	  ,EventTime
	  ,[Macro934]
      ,[Macro936]
      ,[Macro596]
      ,[Macro595]
      ,[Macro709]
      ,[Macro712]
      ,[Macro609]
      ,[Macro612]
      ,[Macro933]
      ,[Macro667]
      ,[Macro666]
      ,[Macro827]
      ,[Macro682]
      ,[Macro691]
      ,[Macro652]
      ,[Macro653]
      ,[Macro654]
      ,[Macro655]
      ,[Macro656]
      ,[Macro657]
      ,[Macro658]
      ,[Macro659]
      ,[Macro660]
      ,[Macro661]
      ,[Macro633]
      ,[Macro634]
      ,[Macro635]
      ,[Macro627]
      ,[Macro628]
      ,[Macro623] 
      ,[Macro624]
      ,[Macro625]
      ,[Macro626] 
	  ,''
	  ,''
	  ,''
	  ,''
FROM #TABLE1 
END

DROP TABLE #TABLE1
DROP TABLE #TABLE2
DROP TABLE #TABLE3
"""
cursor.execute(sql)   #执行sql语句
connect.commit()  #提交
cursor.close()   #关闭游标
connect.close()  #关闭连接