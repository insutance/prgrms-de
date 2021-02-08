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
    conn.set_session(autocommit=True)       
    return conn.cursor()

import requests

def extract(url):
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

# lines에서 0번째 index값은 header가 들어가 있으므로 lines=lines[1:] 을 해줬다
# 아니면 transform()함수에서 return lines[1:] 해줘도 될 것 같다.
def load1(lines):
    cur = get_Redshift_connection()
    lines = lines[1:]
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO insutance.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
            cur.execute(sql)


# header가 있다는 가정하에 header_cnt=0으로 설정(첫번째 레코드는 INSERT 하지 않기)
# 계속해서 +1 해주면서 0(=header 레코드)이 아니면 삽입하는 코드.
def load2(lines):
    cur = get_Redshift_connection()
    header_cnt = 0
    for r in lines:
        if r != '' and header_cnt != 0:
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO insutance.name_gender VALUES ('{name}', '{gender}')".format(name=name, gender=gender)
            print(sql)
            cur.execute(sql)
        header_cnt += 1



"""
main
"""
link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load1(lines)