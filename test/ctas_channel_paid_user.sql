/*
구매 사용자 테이블
*/

CREATE TABLE channel_paid_user AS
SELECT ch.channel, COUNT(pd.userid)
FROM raw_data.channel ch
LEFT JOIN(
  SELECT channel, usc.userid
  FROM raw_data.session_transaction st
  JOIN raw_data.user_session_channel usc
  ON st.sessionid = usc.sessionid
) pd
ON ch.channel = pd.channel
GROUP BY 1
ORDER BY 1;