SELECT
    Server_ID,
    DATE_TRUNC('day', Log_Timestamp) AS Log_Date,
    round(AVG(CPU_Utilization),2) AS Avg_CPU,
    MAX(Memory_Usage) AS Peak_Memory,
    round(SUM(Downtime),2) AS Total_Downtime,
    round(AVG(Availability_Percent),2) AS Avg_Availability
FROM {{ ref('int_server_performance') }}
GROUP BY Server_ID, Log_Date


