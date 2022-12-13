from airflow.models import DAG
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from selenium import webdriver as wd
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime
import pandas as pd
import re
import csv

@task()
def extrack_transformation_1():
    #크롬드라이버 열기

    driver = wd.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()

    base_url = 'https://career.programmers.co.kr/job?page=1&order=recent'
    driver.get(base_url)
    time.sleep(1)
    
    #직무 선택버튼
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/button').click()
    time.sleep(1)

    #서버/백엔드, 프론트엔드, 머신러닝, 인공지능(AI), 데이터 엔지니어, DBA 직무 체크
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[1]/label/input').click()
    time.sleep(0.1)
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[2]/label/input').click()
    time.sleep(0.1)
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[6]/label/input').click()
    time.sleep(0.1)
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[7]/label/input').click()
    time.sleep(0.1)
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[8]/label/input').click()
    time.sleep(0.1)
    driver.find_element(By.XPATH,'//*[@id="search-form"]/div[2]/div[1]/div/ul/li[9]/label/input').click()
    time.sleep(2)

    #최초페이지 파싱
    page = driver.page_source
    soup = BeautifulSoup(page,'lxml')
    time.sleep(3)

    #페이지 번호 추출
    page_num = []
    a = soup.find_all("span", attrs={"class": "page-link"})
    for i in a:
        page_num.append(re.sub(r"[^0-9]","",i.get_text()))
    page_num = [int(a) for a in page_num if a]

    #기업 공고별 url추출
    page_url_list = []

    #최초페이지 세부 url추출
    page_url_list_temp = [code.get_attribute('href') for i in range(1,21) for code in driver.find_elements(By.CSS_SELECTOR,f"#list-positions-wrapper > ul > li:nth-child({i}) > div.item-body > div.position-title-wrapper > h5 > a")]
    page_url_list = page_url_list + page_url_list_temp

    #페이지 이동 및 세부페이지 url추출(첫 5페이지)
    for j in range(3,7):
        driver.find_element(By.XPATH,f'//*[@id="tab_position"]/div[3]/ul/li[{j}]/span').click()
        time.sleep(1)

        #페이지 공고별 코드추출
        page_url_list_temp = [code.get_attribute('href') for i in range(1,21) for code in driver.find_elements(By.CSS_SELECTOR,f"#list-positions-wrapper > ul > li:nth-child({i}) > div.item-body > div.position-title-wrapper > h5 > a")]
        page_url_list = page_url_list + page_url_list_temp

    #페이지 이동 및 세부페이지 url추출(6페이지~ (마지막-3)페이지)
    rep = 6
    while rep <= (page_num[-1] - 3):
        driver.find_element(By.XPATH,f'//*[@id="tab_position"]/div[3]/ul/li[7]/span').click()
        time.sleep(1)

        #페이지 공고별 코드추출
        page_url_list_temp = [code.get_attribute('href') for i in range(1,21) for code in driver.find_elements(By.CSS_SELECTOR,f"#list-positions-wrapper > ul > li:nth-child({i}) > div.item-body > div.position-title-wrapper > h5 > a")]
        page_url_list = page_url_list + page_url_list_temp
        rep += 1

    #페이지 이동 및 세부페이지 url추출(마지막 3페이지)
    for j in range(6,9):
        driver.find_element(By.XPATH,f'//*[@id="tab_position"]/div[3]/ul/li[{j}]/span').click()
        time.sleep(1)

        #페이지 공고별 코드추출
        page_url_list_temp = [code.get_attribute('href') for i in range(1,21) for code in driver.find_elements(By.CSS_SELECTOR,f"#list-positions-wrapper > ul > li:nth-child({i}) > div.item-body > div.position-title-wrapper > h5 > a")]
        page_url_list = page_url_list + page_url_list_temp

    
    #스케줄링 사용- 검색할 새로운 url만 추출(이전 사용했던 url을 제외)
    #이전 크롤링 때 사용한 url 불러오기
    try: 
        page_url_list_total = []
        with open('list_to_csv.csv','r',newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                page_url_list_total.extend(row)
    except:
        pass
    
    #이전 사용했던 url 제외 및 업데이트
    try: #첫번째 이후 크롤링
        search_url_list = list(set(page_url_list) - set(page_url_list_total))

    except: #첫 크롤링 
        search_url_list = page_url_list


    #추가된 이전사용한 url 저장 
    with open('list_to_csv.csv','a',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(search_url_list)


    return search_url_list

@task()
def extrack_transformation_2(search_url_list):
    #크롬드라이버 열기
    driver = wd.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    
    
    datas = []
    #공고페이지별 data 가져오기
    for i in search_url_list:
        driver.get(i)
        time.sleep(0.5)#멈춰주지 않으면 크롤링할때 오류생김(페이지 가져오는데 시간걸려서 기다려줘야함)

        #예외상황1:근무위치정보 없는경우 
        try:
            근무위치 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text
        except:
            근무위치 = ""    

        #기간정보를 채용시작일과 마감일로 구분
        #예외상황2:기간정보가 상시채용인 경우 오류나기 때문에 try-except활용
        try:
            채용시작일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0].strip())
            채용마감일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[1].strip())
        except:
            채용시작일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0]
            채용마감일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0]

        #예외상황3:연봉이 페이지에 포함될 경우 인덱스 순서가 망가지기 때문에 try-except활용
        if "만원" in 채용시작일:
            try:
                채용시작일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text.split('부터')[0].strip())
                채용마감일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text.split('부터')[1].strip())
            except:
                채용시작일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text.split('부터')[0]
                채용마감일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text.split('부터')[0]

            근무위치 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[6]/td[3]').text

        기업 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/header/div[1]/h4[1]').text
        직무 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[1]/td[3]').text
        경력 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[3]/td[3]').text
        고용형태 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[2]/td[3]').text


        #예외상황4:고용형태가 없을경우 오류 발생(고용형태 내용이 경력에 들어감)
        if not ('정규' in 고용형태) | ('계약' in 고용형태 ):
            경력 =  driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[2]/td[3]').text
            try:
                채용시작일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[3]/td[3]').text.split('부터')[0].strip())
                채용마감일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[3]/td[3]').text.split('부터')[1].strip())
            except:
                채용시작일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[3]/td[3]').text.split('부터')[0]
                채용마감일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[3]/td[3]').text.split('부터')[0]
            근무위치 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text

            if "만원" in 채용시작일:
                try:
                    채용시작일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0].strip())
                    채용마감일 = re.sub(r'[^0-9:\- ]', '', driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[1].strip())
                except:
                    채용시작일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0]
                    채용마감일 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[4]/td[3]').text.split('부터')[0]
                근무위치 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[1]/table/tbody/tr[5]/td[3]').text
            고용형태 = ""




        #예외상황5:기술스택이 포함되어있지 않는 경우 오류 발생 try except 활용
        try:
            업무소개 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[3]/div/div/div').text
            자격조건 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[4]/div/div/div').text

            #예외상황6:우대사항에 타이틀이 아예없는 경우 오류가 발생하므로 try-except 구문 활용
            try:
                우대사항 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[5]/div/div/div/ul[1]').text
            except:
                #예외상황7:우대사항이 아예없는 경우 오류 발생 try-except 활용
                try:
                    우대사항 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[5]/div/div/div').text    
                except:
                    우대사항 = ""

            data = {"채용시작일": 채용시작일,
                    "채용마감일": 채용마감일,
                    "기업": 기업,
                    "직무": 직무,
                    "근무위치": 근무위치,
                    "경력": 경력,
                    "고용형태": 고용형태,
                    "업무소개": 업무소개,
                    "자격조건": 자격조건,
                    "우대사항": 우대사항
                   }

            datas.append(data)
            time.sleep(1.5)#해당 페이지 트래픽 과부하를 방지하여 멈춤시간 갖음

        except:
            업무소개 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[2]/div/div/div').text
            #예외상황8:자격조건,우대사항이 없는 경우 오류 발생 try-except 활용 
            try:
                자격조건 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[3/div/div/div').text

                try:
                    우대사항 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[4]/div/div/div/ul[1]').text
                except:
                    우대사항 = driver.find_element(By.XPATH,'/html/body/div[2]/div/div[1]/div/div[1]/section[4]/div/div/div').text    

                data = {"채용시작일": 채용시작일,
                        "채용마감일": 채용마감일,
                        "기업": 기업,
                        "직무": 직무,
                        "근무위치": 근무위치,
                        "경력": 경력,
                        "고용형태": 고용형태,
                        "업무소개": 업무소개,
                        "자격조건": 자격조건,
                        "우대사항": 우대사항
                       }

                datas.append(data)
                time.sleep(1.5)#해당 페이지 트래픽 과부하를 방지하여 멈춤시간 갖음


            except:

                data = {"채용시작일": 채용시작일,
                    "채용마감일": 채용마감일,
                    "기업": 기업,
                    "직무": 직무,
                    "근무위치": 근무위치,
                    "경력": 경력,
                    "고용형태": 고용형태,
                    "업무소개": 업무소개,
                    "자격조건": "",
                    "우대사항": ""
                   }

                datas.append(data)
                time.sleep(1.5)#해당 페이지 트래픽 과부하를 방지하여 멈춤시간 갖음

    datas_df = pd.DataFrame(datas)
    datas_df = datas_df[['채용시작일','채용마감일','기업','직무','근무위치','경력','고용형태','업무소개','자격조건','우대사항']]
    #데이터 프레임의 형태로 함수를 빠져나가지 못함(딕셔너리 형태로 바꿔서 내보내야함)
    datas_df = datas_df.to_dict()
    return datas_df    

@task()
def load(datas_df):
    conn = BaseHook.get_connection('iegwdewk')#Airflow Connection Id 
    engine = create_engine('postgresql://iegwdewk:1ogh35RHK_jlpXA3G_aRkmWqReT-fgme@raja.db.elephantsql.com/iegwdewk')
    #딕셔너리 형태의 데이터를 다시 데이터 프레임으로 변환
    datas_df = pd.DataFrame(datas_df)
    datas_df.to_sql('datas', engine, if_exists="append", index=False)


with DAG(dag_id="cp2", schedule_interval="0 9 * * *", start_date=datetime(2022, 12, 10), catchup=False) as dag:
    #2022년 12월 10일 이후부터 매일 9시마다 실행(한국시간기준X - UTC시간기준O)
    first_extrack_transformation = extrack_transformation_1()
    second_extrack_transformation = extrack_transformation_2(first_extrack_transformation)
    loads = load(second_extrack_transformation)
        
    first_extrack_transformation >> second_extrack_transformation >> loads