SELECT 
    user_id,
    json_extract_scalar(user_profile, '$.name') AS user_name,
    json_extract_scalar(user_profile, '$.address.city') AS city,
    CAST(json_extract(user_profile, '$.tags') AS ARRAY(VARCHAR)) AS tags
FROM user_data;