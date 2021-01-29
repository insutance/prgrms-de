/*
채널별 유니크한 사용자 요약 테이블 생성 (CTAS 사용)

1) 베이스가 되는 테이블 : channel
why? 채널별로 유니크한 사용자를 알아보고 싶기 때문에

2) 어떤 조인 : LEFT JOIN
why? 채널별로 따져야 하고, 해당 채널로 들어오지 않았을 경우도 있기 때문에
*/

CREATE TABLE channel_unique_user AS
SELECT ch.channel, COUNT(DISTINCT usc.userid)
FROM raw_data.channel ch
LEFT JOIN raw_data.user_session_channel usc
ON ch.channel = usc.channel
GROUP BY 1
ORDER BY 1;