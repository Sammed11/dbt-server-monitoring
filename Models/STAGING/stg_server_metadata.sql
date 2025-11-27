SELECT
    Server_ID,
    Host_Name,
    Server_Cluster,
    Server_Location,
    OS_Type,
    Admin_Name,
    Admin_Email,
    Admin_Phone,
    IP_Address
FROM {{ source('RAW', 'SERVER_METADATA') }}