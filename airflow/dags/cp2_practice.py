import time
from datetime import datetime
from airflow.models import DAG
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine


@task()
def extrack():
    student_card = pd.DataFrame({'ID':[20190103, 20190222, 20190531],
                             'name':['Kim', 'Lee', 'Jeong'],
                             'class':['H', 'W', 'S']})
    #데이터 프레임의 형태로 함수를 빠져나가지 못함(딕셔너리 형태로 바꿔서 내보내야함)
    student_card = student_card.to_dict()
    return student_card

@task()
def load(student_card):
    conn = BaseHook.get_connection('iegwdewk')#Airflow Connection Id 
    engine = create_engine('postgresql://iegwdewk:1ogh35RHK_jlpXA3G_aRkmWqReT-fgme@raja.db.elephantsql.com/iegwdewk')
    #딕셔너리 형태의 데이터를 다시 데이터 프레임으로 변환
    student_card = pd.DataFrame(student_card)
    student_card.to_sql('datas', engine, if_exists="append", index=False)


with DAG(dag_id="codestates_project2", schedule_interval="0 9 * * *", start_date=datetime(2022, 12, 10), catchup=False) as dag:
    #2022년 12월 10일 이후부터 매일 9시마다 실행(한국시간기준X - UTC시간기준O)
    extracks = extrack()
    loads = load(extracks)
        
    extracks >> loads

