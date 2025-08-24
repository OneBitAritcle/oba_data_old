# news_pipeline_select.py
from __future__ import annotations
from datetime import datetime, timedelta
import os
import mysql.connector

from airflow import DAG
from airflow.operators.python import PythonOperator

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "user": os.environ.get("MYSQL_USER", "user"),
    "password": os.environ.get("MYSQL_PASSWORD", "password"),
    "database": os.environ.get("MYSQL_DB", "oba_article"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
}

def get_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)

# Task 함수 정의
def select_mark_top5_task(**context):
    from libs.article_selection import select_and_mark_top5
    conn = get_conn()
    try:
        top5 = select_and_mark_top5(conn)
        if not top5 or len(top5) < 5:
            raise ValueError("Top5 기사 선정 실패: 결과가 5개 미만입니다.")
    finally:
        conn.close()
    return top5

def log_top5_task(**context):
    top5 = context["ti"].xcom_pull(task_ids="select_mark_top5")
    print("[TOP 5]")
    for i, row in enumerate(top5 or [], start=1):
        print(f"{i}. id={row[0]} dup={row[4]} time={row[1]} cat={row[2]} url={row[3]}")

def crawl_contents_task(**context):
    import sys
    sys.path.append("libs")
    from libs.crawler import get_content
    from libs.article_selection import upsert_article_contents

    conn = get_conn()
    try:
        top5 = context["ti"].xcom_pull(task_ids="select_mark_top5")
        urls = [row[3] for row in top5]
        ids = [row[0] for row in top5]
        contents = get_content(urls)    # 본문 크롤링
        for i, content in enumerate(contents):  # id 연결
            content["id"] = ids[i]  
        upsert_article_contents(conn, contents) # 테이블에 본문 저장
    finally:
        conn.close()

# DAG2 정의
default_args = {
    "owner": "news-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="news_pipeline_select",
    description="Select Top5 and mark is_used",
    default_args=default_args,
    start_date=datetime(2025, 8, 19),
    schedule=None,               # DAG1에서 트리거 받도록 유지
    catchup=False,
    max_active_runs=1,
    tags=["news", "select", "etl"],
) as dag2:

    t3 = PythonOperator(
        task_id="select_mark_top5", 
        python_callable=select_mark_top5_task
    )
    t4 = PythonOperator(
        task_id="log_top5", 
        python_callable=log_top5_task
    )
    t5 = PythonOperator(
        task_id="crawl_contents",
        python_callable=crawl_contents_task
    )

    t3 >> t4 >> t5
