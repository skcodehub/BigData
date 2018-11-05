CREATE DATABASE SK_STOCK;

create table VIDEO_DATA(
	video_id STRING, 
	uploaded_by STRING,
	elapsed_time INT,
	category STRING,
	duration INT,
	num_views INT,
	rating FLOAT,
	num_ratings BIGINT,
	num_comments BIGINT,
	rel_video_ids ARRAY<STRING>
 )
 ROW FORMAT DELIMITED 
 FIELDS TERMINATED BY  '\t'
 STORED AS TEXTFILE
 ;
 
 load data inpath '/user/edureka_403927/youtubedata.txt' into table VIDEO_DATA;
 
 /*
 DROP TABLE VIDEO_DATA 
3 minutes ago	
a few seconds ago	
Video_Data	
load data inpath '/user/edureka_403927/youtubedata.txt' into table VIDEO_DATA 
a few seconds ago	
Video_Data	
create table VIDEO_DATA(  video_id STRING,   uploaded_by STRING,  elapsed_time INT,  category STRING,  duration INT,  num_views INT,  rating FLOAT,  num_ratings BIGINT,  num_comments BIGINT,  rel_video_ids STRING ) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' STORED AS TEXTFILE 
a minute ago	
Video_Data	
create table VIDEO_DATA(  video_id STRING,   uploaded_by STRING,  elapsed_time INT,  category STRING,  duration INT,  num_views INT,  rating FLOAT,  num_ratings BIGINT,  num_comments BIGINT,  rel_video_ids STRING ) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LINES TERMINATED BY '\n' 
a minute ago	
Video_Data	


 */
  
SELECT CATEGORY, COUNT(CATEGORY) FROM video_data
WHERE CATEGORY IS NOT NULL
GROUP BY category
;
category	count
UNA 	32
2	Autos & Vehicles	77
3	Comedy	414
4	Education	65
5	Entertainment	908
6	Film & Animation	260
7	Howto & Style	137
8	Music	862
9	News & Politics	333
10	Nonprofits & Activism	42
11	People & Blogs	398
12	Pets & Animals	95
13	Science & Technology	80
14	Sports	251
15	Travel & Events	112

--A.Find out the top 5 categories with maximum number of videos uploaded. Results in Top5cat_capture

with CATG_COUNT AS (SELECT CATEGORY, COUNT(CATEGORY) as CAT_COUNT FROM video_data
WHERE CATEGORY IS NOT NULL
GROUP BY category
)

select category, cat_count from catg_count
order by cat_count desc
limit 5
;

--B.Find out the top 10 rated videos.

select video_id, rating, num_ratings from sandbox.video_data
order by rating desc, num_ratings desc
limit 10;
/*
INFO  : Compiling command(queryId=hive_20180831161010_6c46db51-9223-456c-a1ae-22a8df932e30): select video_id, rating, num_ratings from sandbox.video_data
order by rating desc, num_ratings desc
limit 10
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:video_id, type:string, comment:null), FieldSchema(name:rating, type:float, comment:null), FieldSchema(name:num_ratings, type:bigint, comment:null)], properties:null)
INFO  : Completed compiling command(queryId=hive_20180831161010_6c46db51-9223-456c-a1ae-22a8df932e30); Time taken: 0.14 seconds
INFO  : Executing command(queryId=hive_20180831161010_6c46db51-9223-456c-a1ae-22a8df932e30): select video_id, rating, num_ratings from sandbox.video_data
order by rating desc, num_ratings desc
limit 10
INFO  : Query ID = hive_20180831161010_6c46db51-9223-456c-a1ae-22a8df932e30
INFO  : Total jobs = 1
INFO  : Launching Job 1 out of 1
INFO  : Starting task [Stage-1:MAPRED] in serial mode
INFO  : Number of reduce tasks determined at compile time: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1531777581032_4882
INFO  : Kind: HDFS_DELEGATION_TOKEN, Service: hdfs:nameservice1, Ident: (token for hive: HDFS_DELEGATION_TOKEN owner=hive/XYZ@SYSTEMS, renewer=yarn, realUser=, issueDate=1535731808788, maxDate=1536336608788, sequenceNumber=36364, masterKeyId=1663)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.219:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535731808817, maxDate=1536336608817, sequenceNumber=68455, masterKeyId=2783)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.220:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535731808874, maxDate=1536336608874, sequenceNumber=68456, masterKeyId=2784)
INFO  : The url to track the job: https://XYZ.com:9990/proxy/application_1531777581032_4882/
INFO  : Starting Job = job_1531777581032_4882, Tracking URL = https://xyz.com:9990/proxy/application_1531777581032_4882/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hadoop/bin/hadoop job  -kill job_1531777581032_4882
INFO  : Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
INFO  : 2018-08-31 16:10:23,312 Stage-1 map = 0%,  reduce = 0%
INFO  : 2018-08-31 16:10:34,752 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.05 sec
INFO  : 2018-08-31 16:10:49,277 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.01 sec
INFO  : MapReduce Total cumulative CPU time: 9 seconds 10 msec
INFO  : Ended Job = job_1531777581032_4882
INFO  : MapReduce Jobs Launched: 
INFO  : Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 9.01 sec   HDFS Read: 977771 HDFS Write: 191 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 9 seconds 10 msec
INFO  : Completed executing command(queryId=hive_20180831161010_6c46db51-9223-456c-a1ae-22a8df932e30); Time taken: 42.821 seconds
INFO  : OK
*/

--C.Find out the most viewed videos.

with max_views as (select max(num_views) mxv from sandbox.video_data)

select video_id from video_data vd
join max_views mv
on mv.mxv = vd.num_views
;
/*
INFO  : Kind: HDFS_DELEGATION_TOKEN, Service: hdfs:nameservice1, Ident: (token for hive: HDFS_DELEGATION_TOKEN owner=hive/xyz@SYSTEMS, renewer=yarn, realUser=, issueDate=1535733217724, maxDate=1536338017724, sequenceNumber=36370, masterKeyId=1663)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.219:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535733217801, maxDate=1536338017801, sequenceNumber=68465, masterKeyId=2783)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.220:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535733217754, maxDate=1536338017754, sequenceNumber=68464, masterKeyId=2784)
INFO  : The url to track the job:  https://XYZ.com:9990/proxy/application_1531777581032_4885/
INFO  : Starting Job = job_1531777581032_4885, Tracking URL = https://XYZ.com:9990/proxy/application_1531777581032_4885/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hadoop/bin/hadoop job  -kill job_1531777581032_4885
INFO  : Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
INFO  : 2018-08-31 16:33:47,987 Stage-1 map = 0%,  reduce = 0%
INFO  : 2018-08-31 16:33:58,338 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 4.86 sec
INFO  : 2018-08-31 16:34:09,696 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 10.55 sec
INFO  : MapReduce Total cumulative CPU time: 10 seconds 550 msec
INFO  : Ended Job = job_1531777581032_4885
INFO  : Starting task [Stage-5:MAPREDLOCAL] in serial mode
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
18/08/31 16:34:13 WARN conf.HiveConf: HiveConf of name hive.server2.idle.session.timeout_check_operation does not exist
18/08/31 16:34:13 WARN conf.HiveConf: HiveConf of name hive.sentry.conf.url does not exist
18/08/31 16:34:13 WARN conf.HiveConf: HiveConf of name hive.entity.capture.input.URI does not exist
Execution log at: /tmp/hive/hive_20180831163333_dced30a6-4dd8-4cb0-808f-fc3bee7b7a28.log
2018-08-31 04:34:14	Starting to launch local task to process map join;	maximum memory = 4260102144
2018-08-31 04:34:15	Dump the side-table for tag: 0 with group count: 3126 into file: file:/tmp/hive/33379ca2-e478-49d4-8a49-9c3f0ebf5964/hive_2018-08-31_16-33-37_359_5782661486085447868-31/-local-10004/HashTable-Stage-4/MapJoin-mapfile50--.hashtable
2018-08-31 04:34:15	Uploaded 1 File to: file:/tmp/hive/33379ca2-e478-49d4-8a49-9c3f0ebf5964/hive_2018-08-31_16-33-37_359_5782661486085447868-31/-local-10004/HashTable-Stage-4/MapJoin-mapfile50--.hashtable (120419 bytes)
2018-08-31 04:34:15	End of local task; Time Taken: 1.806 sec.
INFO  : Execution completed successfully
INFO  : MapredLocal task succeeded
INFO  : Launching Job 2 out of 2
INFO  : Starting task [Stage-4:MAPRED] in serial mode
INFO  : Number of reduce tasks is set to 0 since there's no reduce operator
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1531777581032_4886
INFO  : Kind: HDFS_DELEGATION_TOKEN, Service: hdfs:nameservice1, Ident: (token for hive: HDFS_DELEGATION_TOKEN owner=hive/xyz@SYSTEMS, renewer=yarn, realUser=, issueDate=1535733256614, maxDate=1536338056614, sequenceNumber=36372, masterKeyId=1663)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.219:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535733256634, maxDate=1536338056634, sequenceNumber=68467, masterKeyId=2783)
INFO  : Kind: kms-dt, Service: 172.ab.xyz.220:16000, Ident: (kms-dt owner=hive, renewer=yarn, realUser=, issueDate=1535733256675, maxDate=1536338056675, sequenceNumber=68468, masterKeyId=2784)
INFO  : The url to track the job: https://XYZ.com:9990/proxy/application_1531777581032_4886/
INFO  : Starting Job = job_1531777581032_4886, Tracking URL = https://XYZ.com:9990/proxy/application_1531777581032_4886/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-5.14.2-1.cdh5.14.2.p0.3/lib/hadoop/bin/hadoop job  -kill job_1531777581032_4886
INFO  : Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 0
INFO  : 2018-08-31 16:34:26,831 Stage-4 map = 0%,  reduce = 0%
INFO  : 2018-08-31 16:34:36,292 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 3.56 sec
INFO  : MapReduce Total cumulative CPU time: 3 seconds 560 msec
INFO  : Ended Job = job_1531777581032_4886
INFO  : MapReduce Jobs Launched: 
INFO  : Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 10.55 sec   HDFS Read: 977553 HDFS Write: 118 SUCCESS
INFO  : Stage-Stage-4: Map: 1   Cumulative CPU: 3.56 sec   HDFS Read: 5669 HDFS Write: 12 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 14 seconds 110 msec
INFO  : Completed executing command(queryId=hive_20180831163333_dced30a6-4dd8-4cb0-808f-fc3bee7b7a28); Time taken: 60.883 seconds
INFO  : OK
*/