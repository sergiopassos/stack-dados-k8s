import os
from pathlib import Path
from datetime import datetime
from datetime import timedelta

from cosmos import DbtDag, ProjectConfig, ProfileConfig

default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

profile_config = ProfileConfig(
    profile_name="iceberg",
    target_name="dev",
    profiles_yml_filepath=(dbt_root_path / "owshq/profiles.yml")
)

default_args = {
    "owner": "owshq",
    "retries": 1,
    "retry_delay": 0
}

dbt_prj_trn_sql = DbtDag(
    project_config=ProjectConfig(
        dbt_root_path / "owshq",
    ),
    profile_config=profile_config,
    operator_args={
        "append_env": True,
    },

    dag_id="dbt_prj_trn_sql",
    start_date=datetime(2024, 2, 20),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=10),
    default_args=default_args,
    catchup=False,
    render_template_as_native_obj=True
)
