from airflow.decorators import dag, task
from pendulum import datetime
from include.global_variables import global_variables as gv
from include.clients.ft_client import FTClient
from include.clients.minio_client import get_minio_client
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

MINIO = get_minio_client()

@dag(
    start_date=datetime(2025, 9, 1),
    schedule=[gv.DS_START],
    catchup=False,
    max_active_runs=1,
    default_args=gv.default_args,
    tags=["ingestion","france-travail","rome","job", "minio"],
    description="Fetches and ingests ROME job list from France Travail API to MinIO"
)
def in_rome_job_list():
    @task
    def ensure_bucket():
        if not MINIO.bucket_exists(gv.ROME_JOB_BUCKET_NAME):
            MINIO.make_bucket(gv.ROME_JOB_BUCKET_NAME)

    @task()
    def fetch_metiers_list():
        ft = FTClient()

        data = ft.get("rome-fiches-metiers/v1/fiches-rome/fiche-metier")
        print(data)
        return data


    # write the metiers information to MinIO
    write_metiers_list_to_minio = LocalFilesystemToMinIOOperator(
        task_id="write_metiers_list_to_minio",
        bucket_name=gv.ROME_JOB_BUCKET_NAME,
        object_name=gv.CODES_OBJECT_NAME,
        json_serializeable_information=fetch_metiers_list(),
        outlets=[gv.DS_ROME_JOB_DATA_MINIO],
    )

    # set dependencies
    ensure_bucket() >> write_metiers_list_to_minio

in_rome_job_list()
