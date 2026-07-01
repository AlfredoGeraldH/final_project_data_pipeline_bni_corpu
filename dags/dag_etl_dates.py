"""
dag_etl_braches.py
=====================
ETL pipeline: dates.csv → stg_dates → dim_dates

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_dates & dim_dates
    extract_load   (@task Python)            : baca CSV → stg_dates
    transform      (SQLExecuteQueryOperator) : stg_dates → dim_dates

Airflow Connection:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "dag_etl_traininng" # <-- ganti dengan koneksi database yang sudah dibuat di airflow
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "dates.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_dates (
    date_id         INTEGER,
    full_date       VARCHAR(20),
    year            INTEGER,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    week_of_year    INTEGER,
    day_of_month    INTEGER,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    is_weekend      VARCHAR(20),
    is_holiday      VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id         INTEGER PRIMARY KEY NOT NULL,
    full_date       DATE,
    year            INTEGER,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    week_of_year    INTEGER,
    day_of_month    INTEGER,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_dates",
    description         = "ETL dates.csv → stg_dates → dim_dates",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = "0 1 * * *",
    catchup             = False,
    tags                = ["etl", "dates", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/dates"],
)
def dag_etl_dates():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_dates ──────────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_dates"))
            c.commit()

        df.to_sql(
            name      = "stg_dates",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_dates → dim_dates ──────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_dates()
