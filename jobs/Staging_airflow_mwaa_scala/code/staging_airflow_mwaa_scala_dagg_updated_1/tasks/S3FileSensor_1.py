from staging_airflow_mwaa_scala_dagg_updated_1.utils import *

def S3FileSensor_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1",
        bucket_key = [s.strip() for s in "dags/uitesting_shared_SQL_ChildDatabricksShared_REL_DB_MWAA.zip".split(",") if s.strip()],
        bucket_name = "prophecy-airflow-251",
        check_fn = lambda files: {f.get('Size', 0) > 104 for f in files},
        aws_conn_id = "aws_default",
        wildcard_match = False,
        verify = True,
    )
