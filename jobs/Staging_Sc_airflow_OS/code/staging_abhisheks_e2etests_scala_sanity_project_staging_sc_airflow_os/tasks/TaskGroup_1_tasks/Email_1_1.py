from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

def Email_1_1():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1_1",
        to = "sony@prophecy.io",
        subject = "test os",
        html_content = "test os",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "email_default",
    )
