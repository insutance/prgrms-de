"""
#1 SQL이나  Python으로 Monthly Active User 세보기
#2 앞서 주어진 두개의 테이블 (session_timestamp, user_session_channel)을 바탕으로 월별마다 액티브한 사용자들의 수를 카운트한다 (MAU - Monthly Active User)
#3 여기서 중요한 점은 세션의 수를 세는 것이 아니라 사용자의 수를 카운트한다는 점이다.
#4 결과는 예를 들면 아래와 같은 식이 되어야 한다:
        - 2019-05: 400
        - 2019-06: 500
        - 2019-07: 600
"""
import psycopg2
import config

db = config.info

def get_Redshift_connection():
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=db["dbname"],
        user=db["redshift_user"],
        password=db["redshift_pass"],
        host=db["host"],
        port=db["port"]
    ))
    conn.set_session(readonly=True, autocommit=True)
    return conn.cursor()

cur = get_Redshift_connection()

sql = """
        SELECT to_char(ts, 'YYYY-MM') as month , COUNT(distinct userid)
        FROM raw_data.user_session_channel as sc
        JOIN raw_data.session_timestamp as st
        ON sc.sessionid = st.sessionid
        GROUP BY month;
    """
cur.execute(sql)
rows = cur.fetchall()

for r in rows:
    print(r)