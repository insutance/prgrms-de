### 1) 공통점, 차이점에 대한 공부를 해봤는데 잘못되 내용이 있으면 알려주시면 감사하겠습니다:)

**<비교대상>**
- `BEGIN ~ END;`
- `BEGIN WORK(or TRANSACTION) ~ COMMIT WORK(or TRANSACTION)`
(TMI. redshift에서는 `WORK`로 사용, mysql에서는 `TRANS`로 사용)

**<공통점>**</br>
`autocommit=False`로 설정해도 트랜잭션이 실행된다(=DB에 적용된다)
`BEGIN~END` and `BEGIN WORK~COMMIT WORK` 실행 중 오류가 발생하면 사이에 존재하는 모든 작업들을 rollback한다(?)

**<차이점>**</br>
`BEGIN ~ END` 는 트랜잭션을 제어하지 않고 코드 블록을 구분한다. 
각 문은 다른 트랜잭션에서 실행됩니다. (트랜잭션이 실행되지만(=DB에 적용되지만), 하나의 트랜잭션이 아닌 여러개의 트랜잭션이 실행된다.)

`BEGIN WORK ~ COMMIT WORK` 는 트랜잭션의 시작을 나타낸다.
이 블록 내의 각 문은 동일한 트랜잭션에서 실행되며 개별적으로 커밋하거나 롤백 할 수 없다. (트랜잭션이 실행되고, 여러 개의 작업을 한개의 트랜잭션으로 실행한다.)

**<03_use_transaction.py 궁금증>**</br>
`BEGIN~END` 와 `BEGIN WORK~COMMIT WORK` 중 결국 어떤 것을 사용하는 것이 맞는지 궁금합니다..<br>
"`BEGIN~END` and `BEGIN WORK~COMMIT WORK` 실행 중 오류가 발생하면 사이에 존재하는 모든 작업들을 rollback한다(?)" 이 내용이 맞다면 결국 같은 값이 나오기 때문입니다.

</br></br>

### 2) 차이점에 대해 비교해봤는데 잘못되 내용이 있으면 알려주시면 감사하겠습니다:)

**<비교대상>**</br>
- `conn.set_session(autocommit=True)`
- `conn.set_session(autocommit=False)` 

**<차이점>**
- `autocommit=True` 로 한다면 모든 `cur.execute(sql)`을 실행하면 자동으로 커밋된다(=DB에 적용이 된다).
- `autocommit=False` 로 한다면 sql은 실행이 되지만 실제 DB에는 적용되지 않는다.

</br></br>

### 3) '03_2_airflow_porting.py' 파일 내용 질문
13번째 줄을 보면 `'provide_context': True`와 같이 설정을 하고, `extract()`, `transform()`, `load()` 함수 인자에 `**kwargs`를 넣어주는 방식으로 코드를 작성했습니다.

꼭 이렇게 `**kwargs` or `**context`를 사용해야 하는건가요 ?<br>
아래와 같이 코드를 작성한 후 **Airflow Web > Admin > Xcoms** 에 들어가서 확인 했을 때 정상적인 값이 들어갔지만 `transform()`함수에서 어떻게 받아야 하는지 모르겠습니다..
```python
def extract():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    f = requests.get(link)
    return f.text

def tranform():
    # ...?
```
