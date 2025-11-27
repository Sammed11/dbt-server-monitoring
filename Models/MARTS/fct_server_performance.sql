SELECT
    perf.Log_ID,
    perf.Server_ID,
    meta.Host_Name,
    meta.Server_Cluster,
    meta.Server_Location,
    meta.OS_Type,
    perf.Log_Timestamp,
    perf.CPU_Utilization,
    perf.Memory_Usage,
    perf.Disk_IO,
    perf.Network_Traffic_In,
    perf.Network_Traffic_Out,
    perf.Uptime,
    perf.Downtime,
    perf.Availability_Percent
FROM {{ ref('int_server_performance') }} perf
LEFT JOIN {{ ref('stg_server_metadata') }} meta
    ON perf.Server_ID = meta.Server_ID



    