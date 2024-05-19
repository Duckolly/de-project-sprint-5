#version 0.0.3
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dds.user_loader import UserLoader
from dds.restaurant_loader import RestaurantLoader
from dds.timestamp_loader import TimestampLoader 
from dds.products_loader import ProductLoader
from dds.order_loader import OrderLoader
from dds.fct_products_loader import FctProductsLoader 
from dds.dm_curier_loader import CouriersLoader
from dds.dm_deliverys_loader import DeliverysLoader
from dds.to_dds.courier_deliveries_loader import CourierDeliveriesLoader
from lib import ConnectionBuilder

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

def reci_zdravo(log: logging.Logger) -> None:
    log.info("Start")

@dag(
    DAG_id='sprint5_from_stg_to_dds_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds', 'stg_to_dds_dag'],
    is_paused_upon_creation=False
)

def from_stg_to_dds_dag():

    # Создаю подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="zdravo_task")
    def reci_zdravo_task():
        reci_zdravo(log)

    @task(task_id="null_task1")
    def null_task1():
        pass

    @task(task_id="null_task2")
    def null_task2():
        pass

    # # Объявляю таск, который загружает данные.
    @task(task_id="users_load_task")
    def load_users_task():

      # создаю экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываю функцию, которая перельет данные.

    @task(task_id="restaurants_load_task")
    def load_restaurants_task():
        rest_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываю функцию, которая перельет данные.

    @task(task_id="timestamps_load_task")
    def load_timestamps_task():
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываю функцию, которая перельет данные.

    @task(task_id="products_load_task")
    def load_products_task():
        rest_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываю функцию, которая перельет данные.

    @task(task_id="orders_load_task")
    def load_orders_task():
        rest_loader = OrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываю функцию, которая перельет данные.

    @task(task_id="sales_load_task")
    def load_sales_task():
        rest_loader = FctProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_product_facts()  # Вызываю функцию, которая перельет данные.

    @task(task_id="couriers_load_task")
    def load_couriers_task():
        rest_loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываю функцию, которая перельет данные.

    @task(task_id="deliveries_load_task")
    def load_deliveries_task():
        rest_loader = DeliverysLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliverys()  # Вызываю функцию, которая перельет данные.

    @task(task_id="couriers_deliveries_load_task")
    def load_courier_deliveries_task():
        rest_loader = CourierDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    # Инициализирую объявленные таски.
    res_zdravo = reci_zdravo_task()
    res_null1 = null_task1()
    res_null2= null_task2()
    res_users = load_users_task()
    res_restaurants = load_restaurants_task()
    res_timestamps = load_timestamps_task()
    res_products = load_products_task()
    res_orders = load_orders_task()
    res_sales = load_sales_task()
    res_deliveries = load_deliveries_task()
    res_couriers = load_couriers_task()
    res_courier_deliveries = load_courier_deliveries_task()

    res_zdravo >> [res_users , res_restaurants , res_timestamps] >> res_null1 >> [res_products, res_orders] >> res_null2 >> [res_sales, res_deliveries, res_couriers] >> res_courier_deliveries


from_stg_to_dds_dag = from_stg_to_dds_dag()