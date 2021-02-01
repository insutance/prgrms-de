/*
채널별 월 사용자 테이블
*/

CREATE TABLE channel_month AS
SELECT ch.channel, sm.month, COUNT(DISTINCT userid)
FROM raw_data.channel ch
LEFT JOIN (
  SELECT DATE_TRUNC('month', st.ts) AS month, usc.userid, channel
  FROM raw_data.session_timestamp st
  JOIN raw_data.user_session_channel usc
  ON st.sessionid = usc.sessionid
  ORDER BY 1
) sm
ON ch.channel = sm.channel
GROUP BY 2,1
ORDER BY 1,2;