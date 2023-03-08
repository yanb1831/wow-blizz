from utilites.blizz_api import BlizzardApi
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup


api = BlizzardApi(Variable.get("client_id"), Variable.get("client_secret"))
token = api.get_access_token()


def load_token_price(region):
    data = api.get_data_token(region)
    hook = PostgresHook(postgres_conn_id="pg_connection")
    query = """
        INSERT INTO stage.token_prices (last_updated_timestamp,price,region)
        VALUES (%s, %s, %s);
        """
    hook.run(query, parameters=(
        data["last_updated_timestamp"], data["price"], data["region"]))


with DAG(dag_id="get_token_price",
         schedule_interval="@hourly",
         start_date=datetime(2022, 2, 25),
         catchup=False) as dag:

    load_dimension_tasks = list()
    with TaskGroup('get_data_tasks') as get_data_tasks:
        clear_table_task = PostgresOperator(
            task_id="clear_stage_tokens_table",
            postgres_conn_id="pg_connection",
            sql="DELETE FROM stage.token_prices"
        )
        for i in ["eu", "us", "kr", "tw"]:
            load_dimension_tasks.append(
                PythonOperator(
                    task_id=f"{i}_load_token_price",
                    python_callable=load_token_price,
                    op_kwargs={
                        "region": f"{i}",
                    }
                )
            )

    insert_table_task = PostgresOperator(
        task_id="insert_prod_token_prices_table",
        postgres_conn_id="pg_connection",
        sql="sql/token_prices_insert_prod.sql"
    )

    (
        clear_table_task
        >> load_dimension_tasks
        >> insert_table_task
    )
