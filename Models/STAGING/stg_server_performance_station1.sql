SELECT
    Log_ID,
    Server_ID,
    Server_Cluster,
    CAST(Log_Timestamp AS TIMESTAMP_NTZ) AS Log_Timestamp,
    CPU_Utilization,
    Memory_Usage,
    Disk_IO,
    Network_Traffic_In,
    Network_Traffic_Out,
    Uptime,
    Downtime,
    Config_Version,
    CAST(Last_Patch_Date AS TIMESTAMP_NTZ) AS Last_Patch_Date,
    Deployment_Token
FROM {{ source('RAW', 'SERVER_PERFORMANCE_STATION1') }}