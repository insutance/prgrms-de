/*
1) 베이스로 해야하는 테이블 : raw_data.channel

2) 어떤 JOIN ?
    - raw_data.channel 과 조인할 때는 LEFT JOIN을 해야한다. why? 'tiktok' 채널이 있기 때문에
    - user_session_channel 과 session_timestamp는 INNER JOIN or LEFT JOIN 둘 다 해도 된다.
    - session_transaction 과 조인할 때는 LEFT JOIN을 해야한다. INNER JOIN을 하게 되면 transaction이 일어나지 않은 레코드들은 조인되지 않는다.

3) GROUP BY는 channel로 먼저 해준 후, month로 그룹을 해준다.
4) COUNT(DISTINCT userid) : 채널별 월별, 유니크한 사용자 수
5) COUNT(tmp.refunded) : refunded or amount의 개수를 세면 paidUsers를 구할 수 있다.
6) sql에서는 실수로 계산하기 위해서 CONVERT(float,...)를 사용하든지, 처음 계산할 때 'float'형으로 계산하면 된다.
   NULLIF(paiduser,0) : paiduser가 0이면 NULL값을 리턴한다.
   COALESCES()를 통해 NULL이 아닌 값이 나오면 해당 값(나는 0으로 설정함)을 리턴 한다.
7) grossRevenue : refunded 상관없이 총 amount 값
8) NetRevenue : refunded IS TRUE인 값을 만나면 0을 더해준다.
*/

CREATE TABLE summary_channel_month_amount AS
SELECT ch.channel, tmp.month,
        COUNT(DISTINCT tmp.userid) uniqueuser,
        COUNT(tmp.refunded) paiduser,
        COALESCE((100.0*NULLIF(paiduser,0)/uniqueuser),0) conversionrate,      
        SUM(tmp.amount) grossrevenue,
        SUM(CASE tmp.amount
            WHEN tmp.refunded IS TRUE THEN 0
            ELSE tmp.amount
            END) netrevenue
FROM raw_data.channel ch
LEFT JOIN(
  SELECT usc.channel, usc.userid, usc.sessionid, DATE_TRUNC('month',stime.ts) as month, strans.refunded, strans.amount
  FROM raw_data.user_session_channel usc
  LEFT JOIN raw_data.session_timestamp stime ON usc.sessionid = stime.sessionid
  LEFT JOIN raw_data.session_transaction strans ON usc.sessionid = strans.sessionid
) tmp
ON ch.channel = tmp.channel
GROUP BY 1,2
ORDER BY 1,2;