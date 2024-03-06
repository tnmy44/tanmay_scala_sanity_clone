from staging_abhisheks_e2etests_scala_sanity_project_staging_airflow253_scala_composer_job.utils import *

def S3FileSensor_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1",
        bucket_key = [s.strip() for s in "dags/*".split(",") if s.strip()],
        bucket_name = "prophecy-mwaa-243",
        check_fn = None,
        aws_conn_id = "aws_default",
        wildcard_match = True,
        verify = True,
    )
