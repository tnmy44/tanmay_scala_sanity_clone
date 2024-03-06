def HTTPSensor_1():
    from airflow.providers.http.sensors.http import HttpSensor
    from datetime import timedelta

    return HttpSensor(
        task_id = "HTTPSensor_1",
        endpoint = "/webhp",
        request_params = None,
        response_check = None,
        http_conn_id = "rl6j7X9mDUkIrZzdfPx5B",
        poke_interval = 60,
    )
