from datetime import timedelta

default = {
    "owner" : "Kelompok 3",
    "depends_on_past": False,
    "retries": 3,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=2),
}

