from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

import psycopg2
from datetime import datetime, timedelta

"""
# 아래 함수 get_Redshift_connection()은 앞에서 했던 실습했던 것과 똑같다.
# 코드를 단순화 시키면서 정보를 바깥(Airflow Web UI)으로 빼서 코드도 안전해진다.
"""
def get_Redshift_connection():
    # PostgresHook()을 통해 postgres_conn_id의 파라미터의 인자로 
    # Airflow Web UI -> Connections 에서 만들었던 'Conn Id'를 넘겨준다.
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()                 # .get_conn() : Return 값은 'Connection_Object'이다.


def ctas_summary_table(**context):
    schema = context['params']['schema']            # task에서 params로 넘겨준 값을 받아오기
    table_name = context['params']['table_name']

    cur = get_Redshift_connection()                 # Redshift 연결
    sql = "BEGIN;"                                  # 오류가 발생하면 rollback을 해야하므로 Transaction 설정
    
    # 'IF EXISTS'를 사용해 만들려고 하는 테이블이 이미 존재한다면 해당 테이블 삭제하기
    sql += "DROP TABLE IF EXISTS {schema}.{table_name};".format(schema=schema, table_name=table_name)   
    
    # CTAS 코드 : 채널별 월 매출액 테이블 만들기
    sql += """
            CREATE TABLE {schema}.{table_name} AS
            SELECT LEFT(ts, 7) "month",
                c.channel,
                COUNT(DISTINCT userid) uniqueUsers,
                COUNT(DISTINCT CASE WHEN amount > 0 THEN userid END) paidUsers,
                ROUND(paidUsers::decimal*100/NULLIF(uniqueUsers, 0),2) conversionRate,
                SUM(amount) grossRevenue,
                SUM(CASE WHEN refunded is False THEN amount END) netRevenue
            FROM raw_data.channel c
            LEFT JOIN raw_data.user_session_channel usc ON c.channel = usc.channel
            LEFT JOIN raw_data.session_transaction st ON st.sessionid = usc.sessionid
            LEFT JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """.format(schema=schema, table_name=table_name)
    sql += "END;"       # BEGIN ~ END 를 사용해 Transaction 설정
    cur.execute(sql)


dag = DAG(
    dag_id = 'CTAS_Summary_Table',
    start_date = datetime(2021,2,9),
    schedule_interval = '0 2 * * *',
    max_active_runs = 1,
    default_args = {
        'owner' : 'insutance',
        'email' : ['insutance@naver.com'],
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


ctas = PythonOperator(
    task_id = 'ctas_summary_table',
    python_callable = ctas_summary_table,
    params = {
        'schema' : Variable.get('schema'),                  # Variable을 통해 값을 가져오기.
        'table_name' : Variable.get('summary_table_name')   # Airflow Web UI에서 설정할 수 있음. 
    },
    provide_context = True,     # ctas_summary_table() 함수에 params 값을 전달해주기 위해 True로 설정해 함.
    dag = dag
)