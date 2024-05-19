import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder

from cdm.to_cmd.settlement_report import SettlementLoader


@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)
def sprint5_case_cdm_settlement_report():
    @task
    def settlement_daily_report_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
        rest_loader = SettlementLoader(dwh_pg_connect)
        rest_loader.load_data()

    settlement_daily_report_load()  # type: ignore


my_dag = sprint5_case_cdm_settlement_report()  # noqa
