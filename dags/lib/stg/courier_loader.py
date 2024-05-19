import datetime as dt
from logging import Logger

from psycopg import Connection

from lib import PgConnect
from lib.dict_util import json2str
from lib.stg.stg_setting_repository import EtlSetting, StgEtlSettingsRepository
from lib.stg.api import Api


class CouriersDestRepository:
    def insert_courier(self, conn: Connection, courier: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        name = EXCLUDED.name;
                """,
                (courier['_id'], courier['name'])
                # {
                #     "courier_id": courier['_id'],
                #     "name": courier['name']
                # },
            )


class CouriersLoader:
    WF_KEY = "couriers_http_to_stg_workflow"
    NUM_LOADED_KEY = "number_loaded"
    _LOG_THRESHOLD = 10

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CouriersDestRepository()
        self.settings_repository = StgEtlSettingsRepository(pg_dest)
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_settings(self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.NUM_LOADED_KEY: 0})

            self.log.info(f"Total loaded before {wf_setting.workflow_settings[self.NUM_LOADED_KEY]}")
            
            load_queue = Api().get_couriers(offset=wf_setting.workflow_settings[self.NUM_LOADED_KEY])
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            i = 0
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.NUM_LOADED_KEY] = len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(self.WF_KEY, wf_setting_json)
