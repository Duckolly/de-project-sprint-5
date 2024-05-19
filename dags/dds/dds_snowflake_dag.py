import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from config_const import ConfigConst
from lib import ConnectionBuilder

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from dds.fct_products_loader import FctProductsLoader
from dds.order_loader import OrderLoader
from dds.products_loader import ProductLoader
from dds.restaurant_loader import RestaurantLoader
from dds.schema_ddl import SchemaDdl
from dds.timestamp_loader import TimestampLoader
from dds.user_loader import UserLoader
from dds.update_bonuse import BonusLoader
from dds.dm_curier_loader import CouriersLoader
from dds.dm_deliverys_loader import DeliverysLoader

log = logging.getLogger(__name__)

with DAG(
    DAG_id='sprint5_case_dds_snowflake',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as DAG:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    settings_repository = DdsEtlSettingsRepository()

    @task(task_id="schema_init")
    def schema_init(ds=None, **kwargs):
        rest_loader = SchemaDdl(dwh_pg_connect)
        rest_loader.init_schema()
    
    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()
    
    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()
    
    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        couriers_loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        couriers_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dms = load_couriers()
    
    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()
    
    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()

    @task(task_id="deliverys_load")
    def load_deliverys():
        # создаем экземпляр класса, в котором реализована логика.
        delivery_loader = DeliverysLoader(dwh_pg_connect, dwh_pg_connect, log)
        delivery_loader.load_deliverys()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    deliverys_dms = load_deliverys()


    @task(task_id="facts_load")
    def load_facts():
        # создаем экземпляр класса, в котором реализована логика.
        facts_loader = FctProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        facts_loader.load_product_facts()  # Вызываем функцию, которая перельет данные.
        bonus_loader = BonusLoader(dwh_pg_connect, dwh_pg_connect, log)
        bonus_loader.load_bonus()
    # Инициализируем объявленные таски.
    facts_dms = load_facts()

    init_schema = schema_init()
    dm_restaurants = load_dm_restaurants()
    dm_products = load_dm_products()
    dm_timestamps = load_dm_timestamps()
    dm_users = load_dm_users()
    couriers_dms = load_couriers()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()
    deliverys_dms = load_deliverys()
    facts_dms = load_facts()

    # Установка зависимостей
    init_schema >> [dm_restaurants, dm_timestamps, dm_users, dm_products, dm_orders]
    dm_restaurants >> [dm_products, dm_orders]
    dm_users >> dm_orders
    dm_products >> fct_order_products
    dm_orders >> fct_order_products

    # init_schema = schema_init()
    # dm_restaurants = load_dm_restaurants()
    # dm_products = load_dm_products()
    # dm_timestamps = load_dm_timestamps()
    # dm_users = load_dm_users()
    # dm_orders = load_dm_orders()
    # fct_order_products = load_fct_order_products()

    # init_schema >> dm_restaurants  # type: ignore
    # init_schema >> dm_timestamps  # type: ignore
    # init_schema >> dm_users  # type: ignore
    # init_schema >> dm_products  # type: ignore
    # init_schema >> dm_orders  # type: ignore
    
    # dm_restaurants >> dm_products  # type: ignore
    # dm_restaurants >> dm_orders  # type: ignore
    # dm_timestamps #>> dm_orders  # type: ignore
    # dm_users >> dm_orders  # type: ignore
    # dm_products >> fct_order_products  # type: ignore
    # dm_orders >> fct_order_products  # type: ignore
