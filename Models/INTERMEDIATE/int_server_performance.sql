WITH station1 AS (
    SELECT
        Log_ID,
        Server_ID,
        Server_Cluster,
        Log_Timestamp,
        COALESCE(CPU_Utilization, 0) AS CPU_Utilization,
        COALESCE(Memory_Usage, 0) AS Memory_Usage,
        COALESCE(Disk_IO, 0) AS Disk_IO,
        COALESCE(Network_Traffic_In, 0) AS Network_Traffic_In,
        COALESCE(Network_Traffic_Out, 0) AS Network_Traffic_Out,
        COALESCE(Uptime, 0) AS Uptime,
        COALESCE(Downtime, 0) AS Downtime
    FROM {{ ref('stg_server_performance_station1') }}
),
station2 AS (
    SELECT
        Log_ID,
        Server_ID,
        Server_Cluster,
        Log_Timestamp,
        COALESCE(CPU_Utilization, 0) AS CPU_Utilization,
        COALESCE(Memory_Usage, 0) AS Memory_Usage,
        COALESCE(Disk_IO, 0) AS Disk_IO,
        COALESCE(Network_Traffic_In, 0) AS Network_Traffic_In,
        COALESCE(Network_Traffic_Out, 0) AS Network_Traffic_Out,
        COALESCE(Uptime, 0) AS Uptime,
        COALESCE(Downtime, 0) AS Downtime
    FROM {{ ref('stg_server_performance_station2') }}
)

SELECT
    COALESCE(station1.Log_ID, station2.Log_ID) AS Log_ID,
    COALESCE(station1.Server_ID, station2.Server_ID) AS Server_ID,
    COALESCE(station1.Server_Cluster, station2.Server_Cluster) AS Server_Cluster,
    COALESCE(station1.Log_Timestamp, station2.Log_Timestamp) AS Log_Timestamp,
    COALESCE(station1.CPU_Utilization, station2.CPU_Utilization) AS CPU_Utilization,
    COALESCE(station1.Memory_Usage, station2.Memory_Usage) AS Memory_Usage,
    COALESCE(station1.Disk_IO, station2.Disk_IO) AS Disk_IO,
    COALESCE(station1.Network_Traffic_In, station2.Network_Traffic_In) AS Network_Traffic_In,
    COALESCE(station1.Network_Traffic_Out, station2.Network_Traffic_Out) AS Network_Traffic_Out,
    COALESCE(station1.Uptime, station2.Uptime) AS Uptime,
    COALESCE(station1.Downtime, station2.Downtime) AS Downtime,
    round((COALESCE(station1.Uptime, station2.Uptime) / 
     NULLIF(COALESCE(station1.Uptime, station2.Uptime) + COALESCE(station1.Downtime, station2.Downtime), 0)) * 100 ,2)
     AS Availability_Percent
FROM station1
FULL OUTER JOIN station2
ON station1.Server_ID = station2.Server_ID
AND station1.Log_Timestamp = station2.Log_Timestamp