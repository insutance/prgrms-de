/*
월별 유니크한 사용자 요약 테이블 생성 (CTAS 사용)

1) 베이스가 되는 테이블 : session_timestamp
why? 날짜 정보가 있는 테이블은 결국 session_timestamp이기 때문에

2) 어떤 조인 : INNER JOIN or LEFT JOIN
*/

CREATE TABLE month AS
SELECT DATE_TRUNC('month', st.ts) AS month, COUNT(DISTINCT usc.userid)
FROM raw_data.session_timestamp st
LEFT JOIN raw_data.user_session_channel usc
ON usc.sessionid = st.sessionid
GROUP BY 1
ORDER BY 1;