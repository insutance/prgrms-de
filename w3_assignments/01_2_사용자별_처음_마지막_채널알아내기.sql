-- 방법1
SELECT userid,
    MAX(CASE WHEN rn1 = 1 THEN channel END) first_touch,
    MAX(CASE WHEN rn2 = 1 THEN channel END) last_touch
FROM (
    SELECT userid, channel, (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY  st.ts asc)) AS rn1,
    (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY  st.ts desc)) AS rn2
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
)
-- WHERE rn1 = 1 or rn2=1
GROUP BY 1;


-- 방법2
WITH cte AS (
    SELECT userid, channel, (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY  st.ts asc)) AS rn1, (ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY  st.ts desc))AS rn2
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
)
SELECT userid, MAX(CASE WHEN rn1 = 1 THEN channel END) first_touch,
        MAX(CASE WHEN rn2 = 1 THEN channel END) last_touch
FROM cte
WHERE rn1 = 1 or rn2=1
GROUP BY 1;


-- 방법3
SELECT DISTINCT A.userid,
    FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts
    rows between unbounded preceding and unbounded following) AS First_Channel,
    LAST_VALUE(A.channel) over(partition by A.userid order by B.ts
    rows between unbounded preceding and unbounded following) AS Last_Channel
FROM raw_data.user_session_channel A
LEFT JOIN raw_data.session_timestamp B
ON A.sessionid = B.sessionid;



-- 1기 분께서 짰던 SQL
SELECT    A.userid
      ,  SUBSTRING(MIN(CONCAT(RPAD(REPLACE(B.ts, '.', ''), 22, '0'), A.channel)), 23) AS first_channel
      ,  SUBSTRING(MAX(CONCAT(RPAD(REPLACE(B.ts, '.', ''), 22, '0'), A.channel)), 23) AS last_channel
FROM raw_data.user_session_channel A
LEFT JOIN raw_data.session_timestamp B ON    A.sessionid = B.sessionid
GROUP BY 1
ORDER BY 1;


