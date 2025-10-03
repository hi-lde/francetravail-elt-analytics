
from minio import Minio
from include.global_variables.global_variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_IP


def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client
