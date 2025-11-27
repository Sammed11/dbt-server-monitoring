SELECT
    perf.Server_ID,
    meta.Host_Name,
    meta.Server_Cluster,
    meta.Server_Location,
    meta.OS_Type,
    perf.Log_Date,
    perf.Avg_CPU,
    perf.Peak_Memory,
    perf.Total_Downtime,
    perf.Avg_Availability
FROM {{ ref('int_server_daily_summary') }} perf
LEFT JOIN {{ ref('stg_server_metadata') }} meta
    ON perf.Server_ID = meta.Server_ID