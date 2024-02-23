"""
"""

import os
import logging
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from cosmos import (
    DbtDag,
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    LoadMode
)

logger = logging.getLogger(__name__)
doc_md = """
"""

default_args = {
    "owner": "owshq",
    "retries": 1,
    "retry_delay": 0
}

default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

profile_config = ProfileConfig(
    profile_name="iceberg",
    target_name="dev",
    profiles_yml_filepath=(dbt_root_path / "owshq/profiles.yml")
)


@dag(
    dag_id="dbt_sql_transform",
    doc_md=doc_md,
    start_date=datetime(2024, 2, 20),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=10),
    default_args=default_args,
    catchup=False,
    render_template_as_native_obj=True
)
def dbt_sql_transform():
    """
    """

    tg_stg_mssql = DbtTaskGroup(
        group_id="tg_stg_mssql",
        project_config=ProjectConfig((dbt_root_path / "owshq").as_posix()),
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM,
            select=["tag:mssql"],
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            "vars": {}
        }
    )

    with TaskGroup(group_id="tg_dbt_postgres_models") as tg_dbt_postgres_models:

        dbt_render_config = RenderConfig(
                emit_datasets=False,
                select=[f"tag:postgres"],
            )
        
        tg_stg_postgres = DbtTaskGroup(
            group_id="tg_stg_postgres",
            project_config=ProjectConfig((dbt_root_path / "owshq").as_posix()),
            render_config=dbt_render_config,
            profile_config=profile_config,
            operator_args={
                "install_deps": True,
                "vars": {}
            }
        )

        tg_stg_postgres

    tg_stg_mongodb = DbtTaskGroup(
        group_id="tg_stg_mongodb",
        project_config=ProjectConfig((dbt_root_path / "owshq").as_posix()),
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM,
            select=["tag:mongodb"],
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            "vars": {}
        }
    )

    tg_stg_mssql >> tg_dbt_postgres_models >> tg_stg_mongodb


dbt_sql_transform()
