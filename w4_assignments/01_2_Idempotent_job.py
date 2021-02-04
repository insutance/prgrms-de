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
    return lines[1:]

def load(lines):
    cur = get_Redshift_connection()
    # 멱등성 보장하기 위해서는 삽입하기 전에 테이블에 있는 레코드들을 삭제하면 된다.
    # 단, 오류가 나지 않는다는 가정하에 (오류가 발생하면 모든 레코드들이 삭제되었다가 애매하게 삽입될 수 있다.)
    delete_sql = "DELETE FROM insutance.name_gender;"
    cur.execute(delete_sql)
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO insutance.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
            cur.execute(sql)


"""
main
"""
link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)