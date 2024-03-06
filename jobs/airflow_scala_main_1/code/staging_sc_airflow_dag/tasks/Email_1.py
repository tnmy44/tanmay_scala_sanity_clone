from staging_sc_airflow_dag.utils import *

def Email_1():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1",
        to = "sony@prophecy.io",
        subject = "test subject",
        html_content = "content of email",
        cc = "abhisheks@prophecy.io",
        bcc = "sony+1@prophecy.io",
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "email_default",
    )
