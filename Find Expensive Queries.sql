/* 
----------------------------------------------------------------------------------------------------
SQL Server 2017 - Query Performance Tuning - Grant Fritchey
----------------------------------------------------------------------------------------------------
How to analyze a query execution plan?

1. Focus attention on the nodes with the highest relative cost.
2. Observe the thickness of the connecting arrows between nodes.
A thick connecting arrow indicates a large number of rows being
transferred between the corresponding nodes. Analyze the node
to the left of the arrow to understand why it requires so many
rows.
3. Check the properties of the arrows too. You may see that the
estimated rows and the actual rows are different. This can be caused
by out-of-date statistics, among other things. If you see thick arrows
through much of the plan and then a thin arrow at the end, it might
be possible to modify the query or indexes to get the filtering done
earlier in the plan.
4. Look for hash join operations. For small result sets, a nested loop join
is usually the preferred join technique
5. Look for key lookup operations. A lookup operation for a large
result set can cause a large number of random reads. 
6. There may be warnings, indicated by an exclamation point on one
of the operators, which are areas of immediate concern. These can
be caused by a variety of issues, including a join without join criteria
or an index or a table with missing statistics.
7. Look for steps performing Sort Operations
8. Watch for operators that may be placing additional load on the
system such as table spools.

To examine a costly step in an execution plan further, you should analyze the data
retrieval mechanism for the relevant table or index. First, you should check whether an
index operation is a seek or a scan. Usually, for best performance, you should retrieve
as few rows as possible from a table, and an index seek is frequently the most efficient
way of accessing a small number of rows. A scan operation usually indicates that a larger
number of rows have been accessed. Therefore, it is generally preferable to seek rather
than scan. However, this is not saying that seeks are inherently good and scans are
inherently bad. The mechanisms of data retrieval need to accurately reflect the needs
of the query. A query retrieving all rows from a table will benefit from a scan where a
seek for the same query would lead to poor performance. The key here is understanding
the details of the operations through examination of the properties of the operators to
understand why the optimizer made the choices that it did.

----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------

*/
/*
----------------------------------------------------------------------------------------------------
Taken from Pro SQL Server Internals 2016 - Chapter 7 - Designing and Tuning Indexes

SQL Server tracks execution statistics for queries and exposes them via the sys.dm_exec_query_stats
DMV. Querying this DMV is, perhaps, the easiest way to find the most expensive queries in the system.
Below is a sample query that returns information about the fifty most expensive queries in a
system in terms of the average I/O per execution.
----------------------------------------------------------------------------------------------------
*/


select top 50
substring(qt.text, (qs.statement_start_offset/2)+1,
((
case qs.statement_end_offset
when -1 then datalength(qt.text)
else qs.statement_end_offset
end - qs.statement_start_offset)/2)+1) as [Sql]
,qs.execution_count as [Exec Cnt]
,(qs.total_logical_reads + qs.total_logical_writes)
/ qs.execution_count as [Avg IO]
,qp.query_plan as [Plan]
,qs.total_logical_reads as [Total Reads]
,qs.last_logical_reads as [Last Reads]
,qs.total_logical_writes as [Total Writes]
,qs.last_logical_writes as [Last Writes]
,qs.total_worker_time as [Total Worker Time]
,qs.last_worker_time as [Last Worker Time]
,qs.total_elapsed_time/1000 as [Total Elps Time]
,qs.last_elapsed_time/1000 as [Last Elps Time]
,qs.creation_time as [Compile Time]
,qs.last_execution_time as [Last Exec Time]
from
sys.dm_exec_query_stats qs with (nolock)
cross apply sys.dm_exec_sql_text(qs.sql_handle) qt
cross apply sys.dm_exec_query_plan(qs.plan_handle) qp
order by
[Avg IO] desc
option (recompile)

/*
----------------------------------------------------------------------------------------------------
SQL Server tracks execution statistics for STORED PROCS and exposes them via the sys.dm_exec_procedure_stats
DMV. Below is a sample query that returns information about the fifty most expensive stored procs in a
system in terms of the average I/O per execution.
----------------------------------------------------------------------------------------------------
*/


select top 50
s.name + '.' + p.name as [Procedure]
,qp.query_plan as [Plan]
,(ps.total_logical_reads + ps.total_logical_writes) /
ps.execution_count as [Avg IO]
,ps.execution_count as [Exec Cnt]
,ps.cached_time as [Cached]
,ps.last_execution_time as [Last Exec Time]
,ps.total_logical_reads as [Total Reads]
,ps.last_logical_reads as [Last Reads]
,ps.total_logical_writes as [Total Writes]
,ps.last_logical_writes as [Last Writes]
,ps.total_worker_time as [Total Worker Time]
,ps.last_worker_time as [Last Worker Time]
,ps.total_elapsed_time as [Total Elapsed Time]
,ps.last_elapsed_time as [Last Elapsed Time]
from sys.procedures as p with (nolock) join sys.schemas s with (nolock) on
p.schema_id = s.schema_id
join sys.dm_exec_procedure_stats as ps with (nolock) on
p.object_id = ps.object_id
outer apply sys.dm_exec_query_plan(ps.plan_handle) qp
order by
[Avg IO] desc
option (recompile);


-- Getting information out of the sys.dm_exec_query_stats view simply
-- requires a query against the DMV.


SELECT s.TotalExecutionCount,
t.text,
s.TotalExecutionCount,
s.TotalElapsedTime,
s.TotalLogicalReads,
s.TotalPhysicalReads
FROM
(
SELECT deqs.plan_handle,
SUM(deqs.execution_count) AS TotalExecutionCount,
SUM(deqs.total_elapsed_time) AS TotalElapsedTime,
SUM(deqs.total_logical_reads) AS TotalLogicalReads,
SUM(deqs.total_physical_reads) AS TotalPhysicalReads
FROM sys.dm_exec_query_stats AS deqs
GROUP BY deqs.plan_handle
) AS s
CROSS APPLY sys.dm_exec_sql_text(s.plan_handle) AS t
ORDER BY s.TotalLogicalReads DESC;



/*
Another way to take advantage of the data available from the execution DMOs is to
use query_hash and query_plan_hash as aggregation mechanisms. While a given stored
procedure or parameterized query might have different values passed to it, changing
query_hash and query_plan_hash for these will be identical (most of the time). This
means you can aggregate against the hash values to identify common plans or common
query patterns that you wouldn’t be able to see otherwise. The following is just a slight
modification from the previous query:*/



SELECT s.TotalExecutionCount,
t.text,
s.TotalExecutionCount,
s.TotalElapsedTime,
s.TotalLogicalReads,
s.TotalPhysicalReads
FROM
(
SELECT deqs.query_plan_hash,
SUM(deqs.execution_count) AS TotalExecutionCount,
SUM(deqs.total_elapsed_time) AS TotalElapsedTime,
SUM(deqs.total_logical_reads) AS TotalLogicalReads,
SUM(deqs.total_physical_reads) AS TotalPhysicalReads
FROM sys.dm_exec_query_stats AS deqs
GROUP BY deqs.query_plan_hash
) AS s
CROSS APPLY
(
SELECT plan_handle
FROM sys.dm_exec_query_stats AS deqs
WHERE s.query_plan_hash = deqs.query_plan_hash
) AS p
CROSS APPLY sys.dm_exec_sql_text(p.plan_handle) AS t
ORDER BY TotalLogicalReads DESC;


/*
---------------------------------------------------------------------------------------------
Live Execution Plans
---------------------------------------------------------------------------------------------
Introduced in SQL Server 2014, the DMV sys.dm_exec_query_profiles actually
allows you to see execution plan operations live, observing the number of rows processed
by each operation in real time. However, in SQL Server 2014, and by default in other
versions, you must be capturing an actual execution plan for this to work.
---------------------------------------------------------------------------------------------
*/


SELECT deqp.physical_operator_name,
deqp.node_id,
deqp.thread_id,
deqp.row_count,
deqp.rewind_count,
deqp.rebind_count
FROM sys.dm_exec_query_profiles AS deqp;

/*
-- Apart from above we can also create an XE Session with event QueryThreadProfile 
--as given below. This will allow us to watch live query statistics as the query gets executed. 
Running it will allow you to watch live execution plans on long-running
queries. However, it does more than that. It also captures row and thread counts for all
operators within an execution plan at the end of the execution of that plan. It’s very low
cost and an easy way to capture those metrics, especially on queries that run fast where
you could never really see their active row counts in a live execution plan. This is data
that you get with an execution plan, but this is much more low cost than capturing a
plan.
*/

CREATE EVENT SESSION QueryThreadProfile
ON SERVER
ADD EVENT sqlserver.query_thread_profile
(WHERE (sqlserver.database_name = N'AdventureWorks2014')),
ADD EVENT sqlserver.sql_batch_completed
(WHERE (sqlserver.database_name = N'AdventureWorks2014'))
WITH (TRACK_CAUSALITY = ON)
GO

/*
Another source for optimizer information is the dynamic management view
sys.dm_exec_query_optimizer_info. This DMV is an aggregation of the optimization
events over time. It won’t show the individual optimizations for a given query, but it
will track the optimizations performed. This isn’t as immediately handy for tuning an
individual query, but if you are working on reducing the costs of a workload over time,
being able to track this information can help you determine whether your query tuning
is making a positive difference, at least in terms of optimization time
*/

SELECT deqoi.counter,
deqoi.occurrence,
deqoi.value
FROM sys.dm_exec_query_optimizer_info AS deqoi;



---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
-- Find Execution Plans from the cache
-- Grant Fritchey - SQL Server 2017
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------


SELECT deps.execution_count,
deps.total_elapsed_time,
deps.total_logical_reads,
deps.total_logical_writes,
deqp.query_plan
FROM sys.dm_exec_procedure_stats AS deps
CROSS APPLY sys.dm_exec_query_plan(deps.plan_handle) AS deqp
WHERE deps.object_id = OBJECT_ID('AdventureWorks2012.dbo.AddressByCity');


---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
-- Find Top IO queries
-- Pro SQL Server Internals 2016
-- Dmitri Korotkevitch
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------

/*
Unfortunately, sys.dm_exec_query_stats does not return any information for queries that do not have
compiled plans cached. Usually this is not an issue, because our optimization targets are not only resource
intensive, but are also frequently executed queries. Plans of these queries usually stay in the cache because
of their frequent reuse. However, SQL Server does not cache plans in cases of statement-level recompiles,
and therefore sys.dm_exec_query_stats misses such queries. You should use Extended Events and/or SQL
Traces to capture them. I usually start with queries from the sys.dm_exec_query_stats function output and
crosscheck the optimization targets with Extended Events later.
*/

select top 50
substring(qt.text, (qs.statement_start_offset/2)+1,
((
case qs.statement_end_offset
when -1 then datalength(qt.text)
else qs.statement_end_offset
end - qs.statement_start_offset)/2)+1) as SQL
,qp.query_plan as [Query Plan]
,qs.execution_count as [Exec Cnt]
,(qs.total_logical_reads + qs.total_logical_writes) / qs.execution_count as [Avg IO]
,qs.total_logical_reads as [Total Reads], qs.last_logical_reads as [Last Reads]
,qs.total_logical_writes as [Total Writes], qs.last_logical_writes as [Last Writes]
,qs.total_worker_time as [Total Worker Time], qs.last_worker_time as [Last Worker Time]
,qs.total_elapsed_time / 1000 as [Total Elapsed Time]
,qs.last_elapsed_time / 1000 as [Last Elapsed Time]
,qs.last_execution_time as [Last Exec Time]
,qs.total_rows as [Total Rows], qs.last_rows as [Last Rows]
,qs.min_rows as [Min Rows], qs.max_rows as [Max Rows]
from
sys.dm_exec_query_stats qs with (nolock)
cross apply sys.dm_exec_sql_text(qs.sql_handle) qt
cross apply sys.dm_exec_query_plan(qs.plan_handle) qp
order by
[Avg IO] desc

------------------------------------------
-- Using sys.dm_exec_procedure_stats
------------------------------------------

/*
SQL Server 2008 and above provide stored procedure – level execution statistics with the sys.dm_exec_
procedure_stats view. It provides similar metrics as sys.dm_exec_query_stats , and it can be used to
determine the most resource intensive stored procedures in the system.
*/


select top 50
db_name(ps.database_id) as [DB]
,object_name(ps.object_id, ps.database_id) as [Proc Name]
,ps.type_desc as [Type]
,qp.query_plan as [Plan]
,ps.execution_count as [Exec Count]
,(ps.total_logical_reads + ps.total_logical_writes) / ps.execution_count as [Avg IO]
,ps.total_logical_reads as [Total Reads], ps.last_logical_reads as [Last Reads]
,ps.total_logical_writes as [Total Writes], ps.last_logical_writes as [Last Writes]
,ps.total_worker_time as [Total Worker Time], ps.last_worker_time as [Last Worker Time]
,ps.total_elapsed_time / 1000 as [Total Elapsed Time]
,ps.last_elapsed_time / 1000 as [Last Elapsed Time]
,ps.last_execution_time as [Last Exec Time]
from
sys.dm_exec_procedure_stats ps with (nolock)
cross apply sys.dm_exec_query_plan(ps.plan_handle) qp
order by
[Avg IO] desc


-----------------------------------------------
-- Amit Bansal's Query Performance Tuning Class
-----------------------------------------------


SELECT  q.[query_hash],
SUBSTRING(t.text, (q.[statement_start_offset] / 2) + 1,
    ((CASE q.[statement_end_offset]
        WHEN -1 THEN DATALENGTH(t.[text])
        ELSE q.[statement_end_offset]
    END - q.[statement_start_offset]) / 2) + 1),
SUM(q.[total_physical_reads]) AS [total_physical_reads]
FROM    sys.[dm_exec_query_stats] AS q
CROSS APPLY sys.[dm_exec_sql_text](q.sql_handle) AS t
GROUP BY q.[query_hash],
SUBSTRING(t.text, (q.[statement_start_offset] / 2) + 1,
    ((CASE q.[statement_end_offset]
        WHEN -1 THEN DATALENGTH(t.[text])
        ELSE q.[statement_end_offset]
    END - q.[statement_start_offset]) / 2) + 1)
ORDER BY SUM(q.[total_physical_reads]) DESC;
GO

-- Query Hash
SELECT  p.[query_plan]
FROM    sys.[dm_exec_query_stats] AS q
CROSS APPLY sys.[dm_exec_query_plan](q.[plan_handle]) AS p
WHERE   q.[query_hash] = 0x0066CA5A8AD90EA0
GO



--This query returns back the queries that use the most IO.
--This can mean that either the query is reading from disk more than usual or occupying and utilizing a large amount of buffer cache.
--These are typical symptoms of queries that do not have the proper indexes or queries that simply read a lot of data.

/**********************************************************
*   top procedures memory consumption per execution
*   (this will show mostly reports &amp; jobs)
***********************************************************/
SELECT TOP 100 *
FROM 
(
    SELECT
         DatabaseName       = DB_NAME(qt.dbid)
        ,ObjectName         = OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)
        ,DiskReads          = SUM(qs.total_physical_reads)   -- The worst reads, disk reads
        ,MemoryReads        = SUM(qs.total_logical_reads)    --Logical Reads are memory reads
        ,Executions         = SUM(qs.execution_count)
        ,IO_Per_Execution   = SUM((qs.total_physical_reads + qs.total_logical_reads) / qs.execution_count)
        ,CPUTime            = SUM(qs.total_worker_time)
        ,DiskWaitAndCPUTime = SUM(qs.total_elapsed_time)
        ,MemoryWrites       = SUM(qs.max_logical_writes)
        ,DateLastExecuted   = MAX(qs.last_execution_time)
        
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    GROUP BY DB_NAME(qt.dbid), OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)

) T
ORDER BY IO_Per_Execution DESC

/**********************************************************
*   top procedures memory consumption total
*   (this will show more operational procedures)
***********************************************************/
SELECT TOP 100 *
FROM 
(
    SELECT
         DatabaseName       = DB_NAME(qt.dbid)
        ,ObjectName         = OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)
        ,DiskReads          = SUM(qs.total_physical_reads)   -- The worst reads, disk reads
        ,MemoryReads        = SUM(qs.total_logical_reads)    --Logical Reads are memory reads
        ,Total_IO_Reads     = SUM(qs.total_physical_reads + qs.total_logical_reads)
        ,Executions         = SUM(qs.execution_count)
        ,IO_Per_Execution   = SUM((qs.total_physical_reads + qs.total_logical_reads) / qs.execution_count)
        ,CPUTime            = SUM(qs.total_worker_time)
        ,DiskWaitAndCPUTime = SUM(qs.total_elapsed_time)
        ,MemoryWrites       = SUM(qs.max_logical_writes)
        ,DateLastExecuted   = MAX(qs.last_execution_time)
        
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    GROUP BY DB_NAME(qt.dbid), OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)
) T
ORDER BY Total_IO_Reads DESC



/**********************************************************
*   top adhoc queries memory consumption total
***********************************************************/
SELECT TOP 100 *
FROM 
(
    SELECT
         DatabaseName       = DB_NAME(qt.dbid)
        ,QueryText          = qt.text       
        ,DiskReads          = SUM(qs.total_physical_reads)   -- The worst reads, disk reads
        ,MemoryReads        = SUM(qs.total_logical_reads)    --Logical Reads are memory reads
        ,Total_IO_Reads     = SUM(qs.total_physical_reads + qs.total_logical_reads)
        ,Executions         = SUM(qs.execution_count)
        ,IO_Per_Execution   = SUM((qs.total_physical_reads + qs.total_logical_reads) / qs.execution_count)
        ,CPUTime            = SUM(qs.total_worker_time)
        ,DiskWaitAndCPUTime = SUM(qs.total_elapsed_time)
        ,MemoryWrites       = SUM(qs.max_logical_writes)
        ,DateLastExecuted   = MAX(qs.last_execution_time)
        
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    WHERE OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid) IS NULL
    GROUP BY DB_NAME(qt.dbid), qt.text, OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)
) T
ORDER BY Total_IO_Reads DESC


/**********************************************************
*   top adhoc queries memory consumption per execution
***********************************************************/
SELECT TOP 100 *
FROM 
(
    SELECT
         DatabaseName       = DB_NAME(qt.dbid)
        ,QueryText          = qt.text       
        ,DiskReads          = SUM(qs.total_physical_reads)   -- The worst reads, disk reads
        ,MemoryReads        = SUM(qs.total_logical_reads)    --Logical Reads are memory reads
        ,Total_IO_Reads     = SUM(qs.total_physical_reads + qs.total_logical_reads)
        ,Executions         = SUM(qs.execution_count)
        ,IO_Per_Execution   = SUM((qs.total_physical_reads + qs.total_logical_reads) / qs.execution_count)
        ,CPUTime            = SUM(qs.total_worker_time)
        ,DiskWaitAndCPUTime = SUM(qs.total_elapsed_time)
        ,MemoryWrites       = SUM(qs.max_logical_writes)
        ,DateLastExecuted   = MAX(qs.last_execution_time)
        
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    WHERE OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid) IS NULL
    GROUP BY DB_NAME(qt.dbid), qt.text, OBJECT_SCHEMA_NAME(qt.objectid,dbid) + '.' + OBJECT_NAME(qt.objectid, qt.dbid)
) T
ORDER BY IO_Per_Execution DESC




/**********************************************************
Took the below query from Pinal Dave's blog
https://blog.sqlauthority.com/2010/05/14/sql-server-find-most-expensive-queries-using-dmv/
***********************************************************/


SELECT TOP 10 SUBSTRING(qt.TEXT, (qs.statement_start_offset/2)+1,
((CASE qs.statement_end_offset
WHEN -1 THEN DATALENGTH(qt.TEXT)
ELSE qs.statement_end_offset
END - qs.statement_start_offset)/2)+1),
qs.execution_count,
qs.total_logical_reads, qs.last_logical_reads,
qs.total_logical_writes, qs.last_logical_writes,
qs.total_worker_time,
qs.last_worker_time,
qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
qs.last_execution_time,
qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_logical_reads DESC -- logical reads
-- ORDER BY qs.total_logical_writes DESC -- logical writes
-- ORDER BY qs.total_worker_time DESC -- CPU time


/**********************************************************
-- Track Expensive Queries - Paul Randal
https://www.sqlskills.com/blogs/paul/tracking-expensive-queries-with-extended-events-in-sql-2008/
**********************************************************/

-- Create an Extended Event

IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = ‘EE_ExpensiveQueries’)
DROP EVENT SESSION EE_ExpensiveQueries ON SERVER;
GO

CREATE EVENT SESSION EE_ExpensiveQueries ON SERVER
ADD EVENT sqlserver.sql_statement_completed
(ACTION (sqlserver.sql_text, sqlserver.plan_handle)
WHERE sqlserver.database_id = 18 /*DBID*/  AND cpu_time > 10000 /*total ms of CPU time*/)
ADD TARGET package0.asynchronous_file_target
(SET FILENAME = N’C:\SQLskills\EE_ExpensiveQueries.xel’, METADATAFILE = N’C:\SQLskills\EE_ExpensiveQueries.xem’)
WITH (max_dispatch_latency = 1 seconds);
GO

ALTER EVENT SESSION EE_ExpensiveQueries ON SERVER STATE = START;
GO

-- the query below will give an xml blob only

SELECT COUNT (*) FROM sys.fn_xe_file_target_read_file
('C:\SQLskills\EE_ExpensiveQueries*.xel’, ‘C:\SQLskills\EE_ExpensiveQueries*.xem', NULL, NULL);
GO

--What's more useful is to pull everything out of the XML blob programmatically using the code below:

SELECT
data.value (
'(/event[@name="sql_statement_completed"]/@timestamp)[1]', 'DATETIME') AS [Time],
data.value (
'(/event/data[@name="cpu"]/value)[1]', 'INT') AS [CPU (ms)],
CONVERT (FLOAT, data.value ('(/event/data[@name='duration']/value)[1]', 'BIGINT')) / 1000000
AS [Duration (s)],
data.value (
'(/event/action[@name="sql_text"]/value)[1]', 'VARCHAR(MAX)') AS [SQL Statement],
'0x' + data.value ('(/event/action[@name="plan_handle"]/value)[1]', 'VARCHAR(100)') AS [Plan Handle]
FROM
(SELECT CONVERT (XML, event_data) AS data FROM sys.fn_xe_file_target_read_file
('C:\SQLskills\EE_ExpensiveQueries*.xel', 'C:\SQLskills\EE_ExpensiveQueries*.xem', null, null)
) entries
ORDER BY [Time] DESC;
GO

-------------------------------------------------------------------------------------------------
-- basic mechanism for capturing stored procedures and batches.
-- SQL Server Execution Plans - Grant Fritchey - Page 79
-------------------------------------------------------------------------------------------------

CREATE EVENT SESSION QueryPerformance ON SERVER
ADD EVENT sqlserver.rpc_completed (
WHERE (sqlserver.database_name = N'AdventureWorks2014')),
ADD EVENT sqlserver.sql_batch_completed (
WHERE (sqlserver.database_name = N'AdventureWorks2014'))
ADD TARGET package0.event_file (SET filename = N'QueryPerformance')
WITH (MAX_MEMORY = 4096 KB,
EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
MAX_DISPATCH_LATENCY = 3 SECONDS,
MAX_EVENT_SIZE = 0 KB,
MEMORY_PARTITION_MODE = NONE,
TRACK_CAUSALITY = OFF,
STARTUP_STATE = OFF);



-------------------------------------------------------------------------------------------------
--Query that returns the 50 most I/O-intensive queries, which have plans cached at the moment of execution
-- Expert SQL Server Transactions and Locking - Dmitri
-------------------------------------------------------------------------------------------------



select top 50
substring(qt.text, (qs.statement_start_offset/2)+1,
((
case qs.statement_end_offset
when -1 then datalength(qt.text)
else qs.statement_end_offset
end - qs.statement_start_offset)/2)+1) as SQL
,qp.query_plan as [Query Plan]
,qs.execution_count as [Exec Cnt]
,(qs.total_logical_reads + qs.total_logical_writes) /
qs.execution_count as [Avg IO]
,qs.total_logical_reads as [Total Reads], qs.last_logical_reads
as [Last Reads]
,qs.total_logical_writes as [Total Writes], qs.last_logical_writes
as [Last Writes]
,qs.total_worker_time as [Total Worker Time], qs.last_worker_time
as [Last Worker Time]
,qs.total_elapsed_time / 1000 as [Total Elapsed Time]
,qs.last_elapsed_time / 1000 as [Last Elapsed Time]
,qs.creation_time as [Cached Time], qs.last_execution_time
as [Last Exec Time]
,qs.total_rows as [Total Rows], qs.last_rows as [Last Rows]
,qs.min_rows as [Min Rows], qs.max_rows as [Max Rows]
from
sys.dm_exec_query_stats qs with (nolock)
cross apply sys.dm_exec_sql_text(qs.sql_handle) qt
cross apply sys.dm_exec_query_plan(qs.plan_handle) qp
order by
[Avg IO] desc



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*
Kimberly L. Tripp's PluralSight course on 
Optimizing AdHoc Statements
*/
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------


-- When the "Distinct Plan Count" is mostly 1 for your queries
-- then you MIGHT consider using forced parameterization.

-- However, before you turn this on - you might want to get
-- more details about the queries that have MULTIPLE plans

-- Review a sampling of the queries (grouping by the query_hash)
-- and see which have the highest *Avg* CPU Time:
SELECT [Query Hash] = [qs2].[query_hash] 
	, [Query Plan Hash]
		= [qs2].[query_plan_hash]
	, [Avg CPU Time]
		= SUM ([qs2].[total_worker_time]) /
		  SUM ([qs2].[execution_count]) 
	, [Example Statement Text]
		= MIN ([qs2].[statement_text])
 FROM (SELECT [qs].*,  
        [statement_text] = SUBSTRING ([st].[text], 
			([qs].[statement_start_offset] / 2) + 1
	    	, ((CASE [statement_end_offset] 
				WHEN - 1 THEN DATALENGTH ([st].[text]) 
				ELSE [qs].[statement_end_offset] 
				END 
		        - [qs].[statement_start_offset]) / 2) + 1) 
		FROM [sys].[dm_exec_query_stats] AS [qs] 
			CROSS APPLY [sys].[dm_exec_sql_text]
				 ([qs].[sql_handle]) AS [st]) AS [qs2]
GROUP BY [qs2].[query_hash]
	, [qs2].[query_plan_hash] 
ORDER BY [Avg CPU Time] DESC;
GO

-- Review a sampling of the queries (grouping by the query_hash)
-- and see which have the highest cumulative effect by CPU Time:
SELECT [qs2].[query_hash] AS [Query Hash]
	, SUM ([qs2].[total_worker_time])
		AS [Total CPU Time - Cumulative Effect]
	, COUNT (DISTINCT [qs2].[query_plan_hash])
		AS [Number of plans] 
	, SUM ([qs2].[execution_count]) AS [Number of executions] 
	, MIN ([qs2].[statement_text]) AS [Example Statement Text]
 FROM (SELECT [qs].*,  
        [statement_text] = SUBSTRING ([st].[text], 
			([qs].[statement_start_offset] / 2) + 1
	    	, ((CASE [statement_end_offset] 
				WHEN - 1 THEN DATALENGTH ([st].[text]) 
				ELSE [qs].[statement_end_offset] 
				END 
		        - [qs].[statement_start_offset]) / 2) + 1) 
		FROM [sys].[dm_exec_query_stats] AS [qs] 
			CROSS APPLY [sys].[dm_exec_sql_text]
				 ([qs].[sql_handle]) AS [st]) AS [qs2]
GROUP BY [qs2].[query_hash] 
ORDER BY [Total CPU Time - Cumulative Effect] DESC;
GO

--------------------------------------------------------------------
-- Few queries shared by Amit Pandey (at the time of Plat Perf)
--------------------------------------------------------------------

--Query 1 : Top 10 total CPU consuming queries

SELECT TOP 10
                    QT.TEXT AS STATEMENT_TEXT,
                    QP.QUERY_PLAN,
                    QS.TOTAL_WORKER_TIME AS CPU_TIME
FROM SYS.DM_EXEC_QUERY_STATS QS
CROSS APPLY SYS.DM_EXEC_SQL_TEXT (QS.SQL_HANDLE) AS QT
CROSS APPLY SYS.DM_EXEC_QUERY_PLAN (QS.PLAN_HANDLE) AS QP
ORDER BY TOTAL_WORKER_TIME DESC


--Query 2 : Top 10 average CPU consuming queries

SELECT TOP 10
                    TOTAL_WORKER_TIME ,
                    EXECUTION_COUNT ,
                    TOTAL_WORKER_TIME / EXECUTION_COUNT AS [AVG CPU TIME] ,
QT.TEXT AS QUERYTEXT
FROM SYS.DM_EXEC_QUERY_STATS QS
CROSS APPLY SYS.DM_EXEC_SQL_TEXT(QS.PLAN_HANDLE) AS QT
ORDER BY QS.TOTAL_WORKER_TIME DESC ;

--Query 3 : Top 10 I/O intensive queries

SELECT TOP 10
                    TOTAL_LOGICAL_READS,
                    TOTAL_LOGICAL_WRITES,
                    EXECUTION_COUNT,
                    TOTAL_LOGICAL_READS+TOTAL_LOGICAL_WRITES AS [IO_TOTAL],
                    QT.TEXT AS QUERY_TEXT,
                    DB_NAME(QT.DBID) AS DATABASE_NAME,
                    QT.OBJECTID AS OBJECT_ID
FROM SYS.DM_EXEC_QUERY_STATS QS
CROSS APPLY SYS.DM_EXEC_SQL_TEXT(SQL_HANDLE) QT
WHERE TOTAL_LOGICAL_READS+TOTAL_LOGICAL_WRITES > 0
ORDER BY [IO_TOTAL] DESC

--Query 4 : Execution count of each query
SELECT 
                    QS.EXECUTION_COUNT,
                    QT.TEXT AS QUERY_TEXT,
                    QT.DBID,
                    DBNAME= DB_NAME (QT.DBID),
                    QT.OBJECTID,
                    QS.TOTAL_ROWS,
                    QS.LAST_ROWS,
                    QS.MIN_ROWS,
                    QS.MAX_ROWS
FROM SYS.DM_EXEC_QUERY_STATS AS QS
CROSS APPLY SYS.DM_EXEC_SQL_TEXT(QS.SQL_HANDLE) AS QT
ORDER BY QS.EXECUTION_COUNT DESC

