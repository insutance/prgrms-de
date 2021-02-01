/*
Gross Revenue가 가장 큰 userID 10개 찾기
1) gross revenue는 refund를 생각하지 않는다.
2) refund 비용이 빠지면 net revenue

3) 베이스가 되는 테이블 : user_session_channel
why? userID를 찾는 것이고, userID별 amount를 알아야하기 때문에 기준으로 함.

4) 어떤 조인 : INNER JOIN
why? session_transaction 테이블에 있는 sessionid와 같은 것들만 가져오면 되기 때문에.
*/

SELECT usc.userid, SUM(amount)
FROM raw_data.user_session_channel usc
JOIN raw_data.session_transaction st
ON usc.sessionid = st.sessionid
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;