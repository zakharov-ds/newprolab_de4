from __future__ import print_function

import time
from builtins import range
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import elasticsearch
import os
from datetime import datetime, timedelta

# Я прошу простить если стиль кода будет вызывать течение крови из глаз
# Обязуюсь исправиться со следующей лабой

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id="dag_search_logs",
    default_args=args,
    schedule_interval="*/15 * * * *",
)


# [START howto_operator_python_kwargs]
def sample_to_str(sample):
    txt_res = ""
    for row in sample:
        txt_res = txt_res + str(row) + "\n"
    return txt_res


def write_file(sample):
    if os.path.isfile("./click_logs.txt"):
        file = open("click_logs.txt","a")
        file.write(sample)
        file.close()
    else:
        file = open("click_logs.txt","w")
        file.write(sample)
        file.close()


def write_logs():
    es = elasticsearch.Elasticsearch(["localhost:9200"])
    res = es.search(index="dmitry.zakharov", body=
                    {"query":
                        {"range":
                            {"@timestamp":
                                {"gte":"now-15m",
                                 "lt":"now",
                                 "time_zone": "UTC"
                                }
                            }
                        }
                    }, size=500)  
    sample = res["hits"]["hits"]
    txt_sample = sample_to_str(sample)
    write_file(txt_sample)
    return "Success write logs from elasticsearch"


task = PythonOperator(
        task_id="task_id_1",
        python_callable=write_logs,
        dag=dag,
    )


task
# [END howto_operator_python_kwargs]
