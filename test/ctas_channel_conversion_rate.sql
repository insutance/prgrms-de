/*
conversionRate Table
*/

/*
방법1
JOIN 안에 있는 SELECT에서 0 인지를 판별해서 넘겨주는 방법
*/
CREATE TABLE channel_conversion_rate AS
SELECT ch.channel, cr.conversionRate
FROM raw_data.channel ch
LEFT JOIN(
  SELECT cuu.channel,
        ISNULL((100.*NULLIF(cpu.count,0)/cuu.count),0) conversionRate
  FROM insutance.channel_unique_user cuu
  JOIN insutance.channel_paid_user cpu
  ON cuu.channel = cpu.channel
  WHERE cpu.count <> 0
) cr
ON ch.channel = cr.channel

/*
방법2
JOIN 할 때는 0인 값은 제외한 후
JOIN 된 후에 ISNULL인지 판별
*/
CREATE TABLE channel_conversion_rate AS
SELECT ch.channel, ISNULL(cr.conversionrate, 0) conversionRate
FROM raw_data.channel ch
LEFT JOIN(
  SELECT cuu.channel, (100.*cpu.count/cuu.count) conversionRate
  FROM insutance.channel_unique_user cuu
  JOIN insutance.channel_paid_user cpu
  ON cuu.channel = cpu.channel
  WHERE cpu.count <> 0
) cr
ON ch.channel = cr.channel
ORDER BY 1;