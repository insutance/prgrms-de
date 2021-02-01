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
    conn.set_session(readonly=False, autocommit=True)
    return conn.cursor()

cur = get_Redshift_connection()

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

users = []
for month in months:
    sql_get_users = "SELECT DISTINCT usc.userid\
                    FROM raw_data.user_session_channel usc\
                    JOIN raw_data.session_timestamp st\
                    ON usc.sessionid = st.sessionid\
                    WHERE TO_CHAR(st.ts, 'YYYY-MM') = %s \
                    ORDER BY 1"

    cur.execute(sql_get_users, month)
    users.append(cur.fetchall())

cohorts = []
for i in range(len(users)):
    cohort = []
    if i == 0:
        cohort.append(len(users[i]))
    else:
        cohort.append(len(set(users[i])-set(users[i-1])))
        
    for j in range(i, len(users)):
        cohort.append(len(set(users[i]) - set(users[j])))
    
    cohorts.append(tuple(cohort))

print(cohorts)
"""
sql_create_table = "CREATE TABLE %s.%s(\
                cohort_month varchar(32) primary key,\
                %s\
                )"

columns = ""
for i in range(len(months)):
    columns += "month" + str(i+1) + " int "
    if len(months) != i+1:
        columns += ","

data = (db["redshift_user"], "summary_cohort", columns)
cur.execute(sql_create_table % data)
"""
