def Email_1():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1",
        to = "sony@porphecy.io",
        subject = "test sub",
        html_content = "test con",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "ATSPdLpyCoWns1X5aXZeO",
    )
