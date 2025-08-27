# news_pipeline_ingest.py
from __future__ import annotations

import os, importlib.util
from pathlib import Path
from datetime import datetime, timedelta

import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Config
CRAWLER_MODULE_PATH = os.environ.get(
    "CRAWLER_MODULE_PATH",
    str(Path(__file__).resolve().parent / "libs" / "crawler.py"),
)
# DEFAULT_CATEGORIES = ["artificial-intelligence", "generative-ai"]

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "user": os.environ.get("MYSQL_USER", "user"),
    "password": os.environ.get("MYSQL_PASSWORD", "password"),
    "database": os.environ.get("MYSQL_DB", "oba_article"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
}

def _load_func_from_py(filepath: str, func_name: str):
    spec = importlib.util.spec_from_file_location("user_module", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, func_name)

def get_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)

# Task 정의
def init_schema_task():
    from libs.article_selection import init_schema as init_schema_util
    conn = get_conn()
    try:
        init_schema_util(conn)
    finally:
        conn.close()

def crawl_links_task(**context):
    # categories = context["params"].get("categories", DEFAULT_CATEGORIES)
    get_articles = _load_func_from_py(CRAWLER_MODULE_PATH, "get_articles_from_categories")
    # links = get_articles(categories)
    links = get_articles()
    if not links:
        raise ValueError("크롤링 결과가 없습니다.")
    return links

def upsert_task(**context):
    from libs.article_selection import upsert_today_links
    links = context["ti"].xcom_pull(task_ids="crawl_links")
    if not links:
        raise ValueError("upsert할 링크가 없습니다.")
    conn = get_conn()
    try:
        upsert_today_links(conn, links)
    finally:
        conn.close()
    return "ok"

# DAG1: 수집 및 업서트
default_args = {
    "owner": "news-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="news_pipeline_ingest",
    description="Crawl & upsert news links",
    default_args=default_args,
    start_date=datetime(2025, 8, 19),
    schedule=None,             
    catchup=False,
    max_active_runs=1,
    tags=["news", "rds", "etl"],
) as dag1:

    t0 = PythonOperator(
        task_id="init_schema", python_callable=init_schema_task
    )
    t1 = PythonOperator(
        task_id="crawl_links", python_callable=crawl_links_task,
        # params={"categories": DEFAULT_CATEGORIES}
    )
    t2 = PythonOperator(
        task_id="upsert_links", python_callable=upsert_task
    )

    # DAG2 자동 실행
    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_news_pipeline_select",
        trigger_dag_id="news_pipeline_select",
        wait_for_completion=False,  # DAG2 실행 결과랑 상관없이 DAG1 종료
                                    # True로 바꾸면 DAG2 실패 시 DAG1도 실패로 처리
        reset_dag_run=True,         # 중복 실행 방지(동일한 execution_date 시)
    )

    t0 >> t1 >> t2 >> trigger_dag2
