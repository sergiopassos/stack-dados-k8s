"""
"""

import os
import logging
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import (
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

    pre_dbt = EmptyOperator(task_id="pre_dbt")

    stg_mssql = DbtTaskGroup(
        group_id="stg_mssql",
        project_config=ProjectConfig((dbt_root_path / "owshq").as_posix()),
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM,
            select=["path:models/stage/mssql_*.sql"]
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            "vars": {}
        }
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> stg_mssql >> post_dbt


dbt_sql_transform()
