#version 0.0.13

import logging
import pendulum

from airflow.decorators import dag, task

from lib import ConnectionBuilder
from lib.stg.courier_loader import CouriersLoader
from lib.stg.stg_setting_repository import StgEtlSettingsRepository

log = logging.getLogger(__name__)


def say_hello(log: logging.Logger) -> None:
    log.info("Hello!")


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'proiject'],
    is_paused_upon_creation=False
)
def http_to_stg_delivery_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="say_hello")
    def say_hello_task():
        say_hello(log)

    @task(task_id="couriers_load")
    def load_couriers():
        couriers_loader = CouriersLoader(dwh_pg_connect, log)
        couriers_loader.load_couriers()

    res_sagen_hallo = say_hello_task()
    res_load_couriers_aufgabe = load_couriers()

    res_sagen_hallo >> res_load_couriers_aufgabe 

project_5_dag = http_to_stg_delivery_system_dag()
