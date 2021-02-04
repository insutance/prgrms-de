from airflow import DAG
from airflow.operators import PythonOperator, DummyOperator
from datetime import datetime, timedelta

import psycopg2
import requests

default_args = {
    'owner' : 'insutance',
    'email' : ['insutance@naver.com'],
    'retry' : 1,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True                 # 이것을 True로 설정 (default_arg에 넣어주던가, 아니면 각 PythonOperator에 넣어줘야 함(?))
}

dag = DAG(
	dag_id = 'name_gender2',
	start_date = datetime(2021,2,3),
	schedule_interval = '0 3 * * *',
    default_args = default_args
    )

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

"""
**context or **kwargs 를 사용하면 된다(?) 차이점 공부해야할 듯...
kwargs['ti'] === kwargs['task_instance'] === context['task_instance']
push 해주는 함수에도 필요하고, pull 하는 함수에도 'provide_context': True 해줘야 함.

참고 링크1 : https://stackoverflow.com/a/46819184
참고 링크2 : https://github.com/trbs/airflow-examples/blob/master/dags/example_xcom.py
"""

# ETL 과정을 분리해서 짠 코드
def extract(**context):
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    f = requests.get(link)
    return f.text

def transform(**context):
    text = context['task_instance'].xcom_pull(task_ids='extract')
    lines = text.split("\n")
    return lines[1:]

def load(**kwargs):
    lines = kwargs['ti'].xcom_pull(task_ids='transform')
    cur = get_Redshift_connection()
    sql = "BEGIN WORK; DELETE FROM insutance.name_gender;"
    for r in lines:
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

extract = PythonOperator(
	task_id = 'extract',
	python_callable = extract,
	dag = dag)

transform = PythonOperator(
	task_id = 'transform',
	python_callable = transform,
	dag = dag)

load = PythonOperator(
	task_id = 'load',
	python_callable = load,
	dag = dag)

end = DummyOperator(dag=dag, task_id='end')

start >> extract >> transform >> load >> end