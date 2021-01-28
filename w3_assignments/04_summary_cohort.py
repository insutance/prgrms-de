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

# '해당 월' 중복제거한 유저들을 가져오는 SQL
sql_get_users = "SELECT DISTINCT usc.userid\
                    FROM raw_data.user_session_channel usc\
                    JOIN raw_data.session_timestamp st\
                    ON usc.sessionid = st.sessionid\
                    WHERE TO_CHAR(st.ts, 'YYYY-MM') = %s \
                    ORDER BY 1"

# INSERT sql
sql_insert_values = "INSERT INTO %s.%s\
                    VALUES (%s)"

all_new_users = []                          # 모든 신규 유저들을 저장해 놓는 빈 배열 생성
for i in range(len(months)):                # 위에서 구한 'months(timestamp를 GROUP BY한 데이터들)' 크기만큼 반복문 실행
    data = ""                               # VALUES 안에 넣을 값을 빈 str로 초기화
    data += "'"+months[i][0] +"'" + ', '    # 값의 처음에는 '해당 월'이 들어간다 ex) '2019-05'
    
    cur.execute(sql_get_users, months[i])   # '해당 월'에 '중복이 제거된' 유저들을 가져온다.
    month_users = cur.fetchall()            
    
    new_users = set(month_users)-set(all_new_users)     # '신규 유저'=='해당월 유저'-'모든 신규 유저'
    data += str(len(new_users))                         # len()을 통해 '신규 유저 수'를 저장
    all_new_users += new_users                          # '신규 유저'를 '모든 신규 유저 배열'에 추가한다.
    
    for j in range(i, len(months)):         
        if i!=j:
            cur.execute(sql_get_users, months[j])                                   # '해당 월' 기준으로 '다음 달 유저'를 가져온다.
            next_month_users = cur.fetchall()
            retention_users = set(new_users).intersection(set(next_month_users))    # '해당 달에서 생긴 신규회원이 유지된 수' = '신규 유저' INNER JOIN '다음 달 유저'
            data += ','+ str(len(retention_users))                                  # INSERT INTO VALUES 할 때 ,(콤마) 필요하기 때문에 저장할 때 같이 저장.
        
    print(data)
    cur.execute(sql_insert_values %(db["redshift_user"], "summary_cohort", data))   # INSERT 실행