from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import requests
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommi = autocommit
    return conn.cursor()

@task
def get_countries_info():
    response = requests.get("https://restcountries.com/v3.1/all")
    if response.status_code == 200:
        data = response.json()
        records = []
        for row in data:
            records.append([row["name"]["official"], row["population"], row["area"]])
        return records
    else:
        logging.error(f"HTTP Error: {response.status_code} - {response.reason}")
        return None

def _create_table(cur, schema, table):
    cur.execute(f"""
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table} (
            country varchar(255),
            population varchar(255),
            area varchar(255)
    );""")

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table)
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES('{r[0]}', '{r[1]}', '{r[2]}');"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")

with DAG(
    dag_id = "Countries_to_Redshift",
    start_date = datetime(2023,12,14),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    results = get_countries_info()
    load("yso0302", "country_info", results)