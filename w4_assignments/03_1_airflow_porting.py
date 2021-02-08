from airflow import DAG
from airflow.operators import PythonOperator, DummyOperator
from datetime import datetime

import psycopg2
import requests

dag = DAG(
	dag_id = 'name_gender',
	start_date = datetime(2021,2,4),
	schedule_interval = '0 1 * * *')

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "insutance"
    redshift_pass = "Insutance!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

# ETL 과정을 하나의 함수에 넣은 코드
def etl_name_gender():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    f = requests.get(link)
    lines = f.text.split("\n")

    cur = get_Redshift_connection()
    sql = "BEGIN WORK; DELETE FROM insutance.name_gender;"
    for r in lines[1:]:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql += "INSERT INTO insutance.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
    sql += "COMMIT WORK;"
    cur.execute(sql)

"""
main
"""
start = DummyOperator(dag=dag, task_id="start")

etl_name_gender = PythonOperator(
	task_id = 'etl_name_gender',
	python_callable = etl_name_gender,
	dag = dag)

end = DummyOperator(dag=dag, task_id='end')

start >> etl_name_gender >> end