import json
from typing import Dict, Optional

from psycopg.rows import class_row
from pydantic import BaseModel
from lib.pg_connect import PgConnect


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_settings(self, etl_key: str) -> Optional[EtlSetting]:
        with self._db.client().cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {
                    'etl_key': etl_key
                }
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, workflow_key: str, workflow_settings: str) -> None:
        with self._db.client().cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                        VALUES (%(etl_key)s, %(etl_setting)s)
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "etl_key": workflow_key,
                        "etl_setting": workflow_settings
                    },
                )