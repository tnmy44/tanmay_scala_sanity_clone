from staging_abhisheks_e2etests_scala_sanity_project_staging_airflow253_scala_composer_job.utils import *

def HTTPSensor_1_1():
    from airflow.providers.http.sensors.http import HttpSensor
    from datetime import timedelta

    return HttpSensor(
        task_id = "HTTPSensor_1_1",
        endpoint = "",
        request_params = None,
        response_check = None,
        http_conn_id = "http_default",
        poke_interval = 60,
    )
