## airflow test 관련 질문
1. linux에서 task를 test할 때는 무조건 한번만 실행되는 건가요?
1. linux환경에서 task를 테스트 할 때 'YYYY-MM-DD' 부분은 execution_date를 설정하는 건가요 ?
2. start_date보다 과거날짜로 실행을 시켰는데, 실행이 되는 이유는 뭔가요 ?


예를 들어  아래와 같이 가정하겠습니다.<br>
이와 같이 테스트를 해봤는데, YYYY-MM-DD가 start_date보다 과거인데도 실행이 되서 질문합니다.

- 현재 날짜 : 2021-02-11
- start_date : 2021-02-09
- schedule_interval = "@daily"

`$ airflow test DAG명 Task명 2021-02-05` <br>
- 실행 됨
- 현재날짜보다 과거이고, start_date보다 과거인데 실행 됨. WHY?
- `execution_date=20210205T000000, start_date=20210211T064144, end_date=20210211T064144`

`$ airflow test DAG명 Task명 2021-02-11` <br>
- 실행 됨
- `execution_date=20210211T000000, start_date=20210211T064404, end_date=20210211T064404`

`$ airflow test DAG명 Task명 2021-02-12` <br>
- 실행 오류
- 현재 날짜보다 미래여서 안 됨.
- `'Execution Date' FAILED: Execution date 2021-02-12T00:00:00+00:00 is in the future (the current date is 2021-02-11T06:44:23.422194+00:00).`


</br>
</br>


## catchup=False 질문
제 생각으로는 catchup=False로 설정한다면 해당 DAG는 실행한 날짜(OFF->ON으로 변경한 날짜)에 한번만 실행이 되어야 한다고 이해했는데 맞나요 ?
아니면 start_date에 설정한 날짜에 한번만 실행이 되나요 ?