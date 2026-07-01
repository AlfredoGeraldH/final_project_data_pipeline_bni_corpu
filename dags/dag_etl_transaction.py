"""
dag_etl_transactions.py
=========================
ETL pipeline:
transactions.csv → stg_transactions → fact_transactions

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_transactions & fact_transactions
    extract_load   (@task Python)            : baca CSV → stg_transactions
    transform      (SQLExecuteQueryOperator) : stg_transactions → fact_transactions

Airflow Connection:
    conn_id = "dag_etl_traininng"
"""


import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ─── Constants ────────────────────────────────────────────────────────────────

CONN_ID = "dag_etl_traininng"

SOURCE_FILE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "include",
    "dataset",
    "transactions.csv"
)


DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id      INTEGER,
    transaction_code    VARCHAR(50),
    account_id          INTEGER,
    customer_id         INTEGER,
    branch_id           INTEGER,
    channel_id          INTEGER,
    transaction_date    VARCHAR(20),
    transaction_at      VARCHAR(30),
    transaction_type    VARCHAR(50),
    amount              VARCHAR(30),
    balance_before      VARCHAR(30),
    balance_after       VARCHAR(30),
    status              VARCHAR(20),
    reference_no        VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      INTEGER PRIMARY KEY,
    transaction_code    VARCHAR(50),
    account_id          INTEGER,
    customer_id         INTEGER,
    branch_id           INTEGER,
    channel_id          INTEGER,
    transaction_date_id INTEGER,
    transaction_date    DATE,
    transaction_at      TIMESTAMP,
    transaction_type    VARCHAR(50),
    amount              NUMERIC(18,2),
    balance_before      NUMERIC(18,2),
    balance_after       NUMERIC(18,2),
    status              VARCHAR(20),
    reference_no        VARCHAR(50),
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_transaction_date
        FOREIGN KEY(transaction_date_id)
        REFERENCES dim_dates(date_id),

    CONSTRAINT fk_transaction_account
        FOREIGN KEY(account_id)
        REFERENCES dim_accounts(account_id),

    CONSTRAINT fk_transaction_branch
        FOREIGN KEY(branch_id)
        REFERENCES dim_branches(branch_id),

    CONSTRAINT fk_transaction_customer
        FOREIGN KEY(customer_id)
        REFERENCES dim_customers(customer_id),

    CONSTRAINT fk_transaction_channel
        FOREIGN KEY(channel_id)
        REFERENCES dim_channels(channel_id)
);
"""

# ─── DAG ──────────────────────────────────────────────────────────────────────

@dag(
    dag_id="dag_etl_transactions",
    description="ETL transactions.csv → stg_transactions → fact_transactions",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date=datetime(2025, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    tags=[
        "etl",
        "transactions",
        "fact",
        "postgresql"
    ],
    template_searchpath=[
        "/opt/airflow/include/sql/transactions"
    ],
)
def dag_etl_transactions():


    # ── Task 1: DDL ───────────────────────────────────────────────────────────

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql=DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_transactions ──────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://"
            f"{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        # Read CSV file
        df = pd.read_csv(SOURCE_FILE)

        # Clear staging table before load
        with engine.connect() as c:
            c.execute(
                text(
                    "TRUNCATE TABLE stg_transactions"
                )
            )
            c.commit()

        # Load dataframe into staging table
        df.to_sql(
            name="stg_transactions",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_transactions → fact_transactions ──────────────
    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id=CONN_ID,
        sql="01_transform.sql",
    )


    # ── Dependencies ─────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform

dag_etl_transactions()