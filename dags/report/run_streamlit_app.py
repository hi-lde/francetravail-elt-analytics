from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime
import os
from include.global_variables import global_variables as gv

APP_PATH = os.getenv(
    "STREAMLIT_APP_PATH",
    "/usr/local/airflow/include/streamlit_app/ft_analytics_app.py",
)

@dag(
    start_date=datetime(2025, 9, 1),
    schedule=[gv.DS_START],  # lance après un load réussi
    catchup=False,
    default_args=gv.default_args,
    tags=["report","streamlit"],
    description="Run Streamlit app for ROME × Marché du travail"
)
def run_streamlit_app():

    run = BashOperator(
        task_id="run_streamlit",
        # --server.address 0.0.0.0 indispensable en container/Codespaces
        bash_command=(
            f"streamlit run {APP_PATH} "
            f"--server.address=0.0.0.0 --server.port=8501"
        )
    )

    run

run_streamlit_app()
