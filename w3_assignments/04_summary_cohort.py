import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))     # config.py import 하기 위해

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
    conn.set_session(readonly=False, autocommit=True)       # CREATE TABLE 하기 위해 readonly를 False로 설정
    return conn.cursor()

cur = get_Redshift_connection()

# 모든 날짜 가져오기 (tuple로 이루어진 배열이 넘어옴 ex) [('2019-05',), ('2019-06',), ...)
sql_get_months ="""
        SELECT TO_CHAR(st.ts, 'YYYY-MM') AS month
        FROM raw_data.user_session_channel usc
        JOIN raw_data.session_timestamp st
        ON usc.sessionid = st.sessionid
        GROUP BY 1
        ORDER BY 1
    """
cur.execute(sql_get_months)
months = cur.fetchall()

# TABLE을 만드는 sql
sql_create_table = "CREATE TABLE %s.%s(\
                cohort_month varchar(32) primary key,\
                %s\
                )"

# CREATE TABLE sql을 실행할 때, 필드명을 'month1', 'month2', ... 설정
fields = ""
for i in range(len(months)):
    fields += "month" + str(i+1) + " int "
    if len(months) != i+1:
        fields += ","

cur.execute(sql_create_table %(db["redshift_user"], "summary_cohort", fields))

# 첫 월은 모두 신규이기 때문에 신규 가져오는 sql
sql_get_users = "SELECT DISTINCT usc.userid\
                    FROM raw_data.user_session_channel usc\
                    JOIN raw_data.session_timestamp st\
                    ON usc.sessionid = st.sessionid\
                    WHERE TO_CHAR(st.ts, 'YYYY-MM') = %s \
                    ORDER BY 1"

# EXCEPT를 사용해 cohort를 계산하는 sql
sql_get_cohorts = "(SELECT DISTINCT usc.userid\
                    FROM raw_data.user_session_channel usc\
                    JOIN raw_data.session_timestamp st\
                    ON usc.sessionid = st.sessionid\
                    WHERE TO_CHAR(st.ts, 'YYYY-MM') = %s\
                    ORDER BY 1)\
                    EXCEPT\
                    (SELECT DISTINCT usc.userid\
                    FROM raw_data.user_session_channel usc\
                    JOIN raw_data.session_timestamp st\
                    ON usc.sessionid = st.sessionid\
                    WHERE TO_CHAR(st.ts, 'YYYY-MM') = %s\
                    ORDER BY 1)"

# INSERT sql
sql_insert_values = "INSERT INTO %s.%s\
                    VALUES (%s)"

for i in range(len(months)):                # 위에서 구한 '월' 크기만큼 반복문 실행
    data = ""   
    data += "'"+months[i][0] +"'" + ', '    # 값의 처음에는 '월'이 들어간다 ex) '2019-05'
    
    if i == 0:  cur.execute(sql_get_users, months[i])                   # '첫 월'은 모든 유저가 신규
    else:       cur.execute(sql_get_cohorts, (months[i], months[i-1]))  # '2번째 월'부터 비교를 한다. ex) 6월달이면 5월달이랑 비교, 7월달이면 6월달이랑 비교
    data += str(len(cur.fetchall()))        # 구한 값을 data에 저장 (int)
    
    for j in range(i, len(months)):         # 해당 달에 들어온 신규유저가 다음 달에 있는지 비교 ex) 6월달이면 (6월,7월), (6월,8월), (6월,9월)... 비교
        if i!=j:
            cur.execute(sql_get_cohorts, (months[i], months[j]))
            data += ','+ str(len(cur.fetchall()))   # INSERT INTO VALUES 할 때 ,(콤마) 필요하기 때문에 저장할 때 같이 저장.
        
    print(data)
    cur.execute(sql_insert_values %(db["redshift_user"], "summary_cohort", data))   # INSERT 실행