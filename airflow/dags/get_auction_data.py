from utilites.blizz_api import BlizzardApi
import io
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

api = BlizzardApi(Variable.get("client_id"), Variable.get("client_secret"))
token = api.get_access_token()


def auc_data(region, realm_id):
    data = api.get_data_auction(region, realm_id)
    hook = PostgresHook(postgres_conn_id="pg_connection")
    up = "DELETE FROM stage.auction"
    hook.run(up)
    try:
        connection = hook.get_conn()
        connection.autocommit = True
        buffer = io.StringIO()
        with connection.cursor() as cursor:
            for lot in data:
                buffer.write("|".join(map(lambda x: str(x), (
                    lot["id"],
                    lot.get("buyout", 0),
                    lot["quantity"],
                    lot["time_left"],
                    lot["item"]["id"]
                    ))) + "\n")
            buffer.seek(0)
            sql = """
                COPY stage.auction (id, buyout, quantity, time_left, item_id)
                FROM STDIN DELIMITER '|' CSV HEADER
                """
            cursor.copy_expert(sql, buffer)
    finally:
        if connection:
            cursor.close()
            connection.close()


with DAG(dag_id="get_auction_data",
         schedule_interval="0 */6 * * *",
         start_date=datetime(2022, 2, 25),
         catchup=False) as dag:

    load_data_task = PythonOperator(
        task_id="load_auction_data_task",
        python_callable=auc_data,
        op_kwargs={
            "region": "eu",
            "realm_id": 1602
        }
    )

    insert_table_task = PostgresOperator(
        task_id="insert_prod_auction_data",
        postgres_conn_id="pg_connection",
        sql="sql/auction_insert_prod.sql"
    )

    (
        load_data_task
        >> insert_table_task
    )
