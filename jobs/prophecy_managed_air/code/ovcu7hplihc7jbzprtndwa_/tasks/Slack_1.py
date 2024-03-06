def Slack_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1",
        text = "slack msg from sanity job",
        channel = "sonytest",
        slack_conn_id = "7k_Cby3g6vOgeKrdy93nb",
    )
