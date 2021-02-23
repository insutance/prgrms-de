# 📌 1) 데이터 파이프라인이란?

데이터 파이프라인은 여러가지 이름이 있다. (`Data Pipeline` = `ETL` = `Data Workflow` = `DAG`)
`Airflow`에서는 Data Pipeline을 `DAG` 라고 부른다.

### 1-1) A data from sources to data component(s)

많은 데이터 소스들에서 DW(Data Warehouse)로 옮겨주는 코드이다.

### 1-2) Data sources

`Click stream`, `call data`, `ads performance data`, `transactions`, `sensor data`, `metadata`, ....</br>
More concrete examples : `production databases`, `log files`, `API`, `stream data(Kafka topic)`

### 1-3) Data componetns

`Data Warehouse`, `cache`, `production database`, `ML components`, ...</br>
More concrete examples : `Data Warehouse`, `Data Lake`, `HDFS`, `S3`, `Kafak topics`, `ElasticSearch`, ...

</br>

# 📌 2) 데이터 엔지니어가 하는 작업

### 2-1) **Raw Data ETL Jobs**

1. 외부와 내부 데이터 소스에서 데이터를 읽어오기 (많은 경우 API를 통하게 됨)
2. 적당한 데이터 포맷 변환 (데이터의 크기가 커지면 Spark 등이 필요해짐)
3. DW(Data Warehouse)에 저장(Load)

### 2-2) Summary/Report Jobs

1. DW로부터 데이터를 읽어 다시 DW에 쓰는 ETL
2. 많은 경우 Raw Data를 읽어서 일종의 Report형태나 Summary형태의 테이블으 다시 만드는 용도

Summary Table의 경우 SQL(CATS를 통해) 만으로 만들어지고, 이는 데이터 분석가가 하는 것이 맞다.</br>
**데이터 엔지니어 관점에서는 어떻게 데이터 분석가들이 편하게 할 수 있는 환경을 만들어 주느냐가 관건이다.**

### 2-3) Production Data Jobs

1. DW로부터 데이터를 읽어 다른 Storage(많은 경우 프로덕션 환경)로 쓰는 ETL</br>
a. Summary 정보가 Production 환경에서 성능 이유로 필요한 경우
2. 이 경우 흔한 타켓 스토리지</br>
a. `Cassandra`/ `HBase`/ `DynamoDB`와 같은 `NoSQL`</br>
b. `MySQL`과 같은 관계형 데이터베이스 (OLTP)</br>
c. `Redis`/ `Memcache`와 같은 캐시</br>
d. `ElasticSearch`와 같은 검색엔진

</br>

# 📌 3) ETL 작업하면서 주의해야할 것

### 1) .csv 파일을 작업할 때 첫번째 줄도 같이 삽입되면 안 된다.

### 2) transaction 설정

하나가 실패하게 되면 전체가 실패되도록 해라.

### 3) 멱등성

데이터 파이프라인을 100번 실행해도 1번 실행한 것과 결과가 같아야 한다.</br>
(ex. 중복된 데이터가 100번 들어갈 수 있다.)

</br>

# 📌 Airflow

### 1) `Airflow` 란 데이터 파이프라인(=ETL)의 작성을 쉽게 해주는 '플랫폼'이다.

### 2) Airflow는 2가지 개념이 있다.

- ETL 실행을 쉽게 해주는 스케줄러
- ETL작성을 쉽게 해주는 프레임워크

### 3) Airflow에서 ETL을 `DAG (Directed Acyclic Graph)` 라고 부른다.

`DAG` 는 `Task(=Operator)` 로 구성되어 있다. 

### 4) Airflow는 하나 이상의 서버로 구성되는 클러스터이다.

스케줄러 Job이나 실행하는 Job의 숫자가 많지 않을 때는 서버 1개로도 충분하다.</br>
근데 어느 시점을 넘어가면 서버를 여러대 붙여서 굉장히 많은 수의 `DAG`를 실행하고 스케줄링해야할 때도 있다.

`DAG` 들을 관리하고, 언제 실행되었고 실행결과가 어떤지 기록을 해야하기 때문에 내부에서 DB가 필요하다. 그래서 Airflow를 처음 설치하면 테스트용으로 `SQLite` 라는 DB가 설치되는데 이 DB는 잘 사용하지 않는다. 그렇기 때문에 `MySQL` or `Postgres`를 별도로 설치해서 Airflow에 정보를 저장해주는 DB로 사용해야한다.

### 5) Airflow는 총 5개의 컴포넌트로 구성된다.

1. Web Server
2. Scheduler
3. Worker (=데이터 파이프라인을 실행시켜주는 것)
4. Database(SQLite가 기본으로 설치됨)
5. Queue(멀티노드 구성인 경우에만 사용됨)

</br>

# 📌 DAG (Directed Acyclic Graph)

- Airflow에서의 데이터 파이프라인(=ETL)
- 여러개의 Tasks들도 표현이 되고 Tasks들 간의 실행 순서를 정해줘야한다.
- Task는 하나의 실행 인스턴스이다.
- Task들 사이에는 루프가 없어야 한다. (=Acyclic 해야한다)

```python
# DAG를 작성할 때 아래 2줄은 꼭 있어야한다고 생각하면 좋다.
from airflow import DAG
from datetime import datetime, timedelta    #스케줄링하느라 시간을 많이 쓰기 때문에 import

default_args = {
   'owner': 'insutance',
   'start_date': datetime(2020, 8, 7, hour=0, minute=00),
   'end_date': datetime(2020, 8, 31, hour=23, minute=00),
   'email': ['insutance@naver.com'],    # 실패했을 때 누구한테 이메일 보낼 것인지
   'retries': 1,    # default retry : 0
   'retry_delay': timedelta(minutes=3),    # retry할 때 3분 delay했다가 실행
}

test_dag = DAG(
   "dag_v1",                        # DAG name
   schedule_interval="0 9 * * *",   # schedule (same as cronjob)
   default_args=default_args        # common settings (위에 작성한)
)

# cron을 사용할 때 설치한 timezone을 따라가기 때문에 default: UTC
```

```python
# Operators Creation Example1
t1 = BashOperator(
   task_id='print_date',
   bash_command='date',
   dag=test_dag)
t2 = BashOperator(
   task_id='sleep',
   bash_command='sleep 5',
   retries=3,
   dag=test_dag)
t3 = BashOperator(
   task_id='ls',
   bash_command='ls /tmp',
   dag=test_dag)

t1 >> t2    # t1을 실행후 t2실행해라
t1 >> t3    # t1을 실행후 t3실행해라
```

```python
# Operators Creation Example2

# DummyOperator() = 아무것도 안 하는 거, 시작과 끝을 marking하기 위해서 가끔 사용
start = DummyOperator(dag=dag, task_id="start", *args, **kwargs)
t1 = BashOperator(
   task_id='ls1',
   bash_command='ls /tmp/downloaded',
   retries=3,
   dag=dag)
t2 = BashOperator(
   task_id='ls2',
   bash_command='ls /tmp/downloaded',
   dag=dag)
end = DummyOperator(dag=dag, task_id='end', *args, **kwargs)

start >> t1 >> end
start >> t2 >> end
```

</br>

# 📌 Airflow를 통해서 데이터 파이프라인을 만들 때 주의사항

### 1) '내가 만든 ETL이 무조건 잘 될거다, 유지 잘 될거다' 라는 환상 X

### 2) 그렇게 큰 데이터 소스가 아니면, 그냥 매번 처음부터 복사해오는 것이 좋다.

하지만 데이터가 크다면 incremental update를 하면 된다.

### 3) 멱등성을 항상 고려해야 한다. (100번 돌든, 1번 돌든 결과는 같아야 한다.)

### 4) Rerunning or Backfilling 하는 것이 쉬워야한다.

여러가지 이슈들로 인해 Job이 실패할 수 있기 때문에 밀렸던 Job들 다시 시작해주는게 쉬워져야 한다. 그래야 데이터 엔지니어 인생이 편해진다.

### 5) metadata를 잘 관리하는 게 좋다

### 6) 주기적으로 데이터 파이프라인을 cleanup

### 7) 오류가 발생했다면 어떻게 해결했는지 적어두는 습관

### 8) Input and Output 을 검증하는 코드가 있으면 좋다.