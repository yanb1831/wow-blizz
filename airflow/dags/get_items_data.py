from utilites.blizz_api import BlizzardApi
import asyncio
import pandas as pd
import io
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

api = BlizzardApi(Variable.get("client_id"), Variable.get("client_secret"))
token = api.get_access_token()


def load_items_data(region):
    hook = PostgresHook(postgres_conn_id="pg_connection")
    up = "DELETE FROM stage.items"
    hook.run(up)
    try:
        connection = hook.get_conn()
        connection.autocommit = True
        buffer = io.StringIO()
        with connection.cursor() as cursor:
            query = "SELECT DISTINCT item_id FROM prod.auction"
            cursor.execute(query)
            items = cursor.fetchall()
            items_id = [item_id[0] for item_id in items]
            data = asyncio.run(api.get_data_items(region, items_id, 99))
            df = pd.json_normalize(data)
            df = df.iloc[:, 0:4]
            df.to_csv(buffer, header=None, index=False, sep="|")
            with connection.cursor() as cursor:
                buffer.seek(0)
                sql = """
                    COPY stage.items (id, name, level, required_level)
                    FROM STDIN DELIMITER '|' CSV HEADER
                """
                cursor.copy_expert(sql, buffer)
    finally:
        if connection:
            cursor.close()
            connection.close()


with DAG(dag_id="get_items_data",
         schedule_interval="0 */6 * * *",
         start_date=datetime(2022, 2, 25),
         catchup=False) as dag:

    wait_data = ExternalTaskSensor(
        task_id="wait_auction_data",
        external_dag_id="get_auction_data",
        external_task_id="insert_prod_auction_data"
    )

    load_items_data_task = PythonOperator(
        task_id="load_items_data_task",
        python_callable=load_items_data,
        op_kwargs={
            "region": "eu"
            }
    )

    insert_table_task = PostgresOperator(
        task_id="insert_prod_items_table",
        postgres_conn_id="pg_connection",
        sql="sql/items_insert_prod.sql"
    )

    (
        wait_data
        >> load_items_data_task
        >> insert_table_task
    )
