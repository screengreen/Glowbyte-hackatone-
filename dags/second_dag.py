from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2




def get_last_dag_runtime(dag):

    Data_file = open('data.txt', 'r')
    prev_dag_run = Data_file.readline()
    Data_file.close()

    Data_file = open('data.txt', 'w')
    Data_file.write(str(dag.latest_execution_date))
    Data_file.close()

    if prev_dag_run is None:
        return '2013-01-01'
    else:
        return prev_dag_run

def change_to_format(st):
    import datetime
    new_st = st.replace('-', '/').strip()
    new_st = datetime.datetime.strptime(new_st[:new_st.find('.')], "%Y/%m/%d %H:%M:%S")
    new_st = new_st + datetime.timedelta(hours=3)
    new_st = str(new_st)
    return new_st


def Update_db():
    date_and_time = str(get_last_dag_runtime(Update_db_dag))
    last_dag_runtime = change_to_format(date_and_time)
    connection = psycopg2.connect(user="etl_tech_user",
                                  database="taxi",
                                  password="etl_tech_user_password",
                                  host="de-edu-db.chronosavant.ru",
                                  port="5432")

    cursor = connection.cursor()
    cursor.execute(f""" SELECT "ride_id", "dt" FROM "main"."rides" WHERE "dt" > '{last_dag_runtime}' """)
    record = cursor.fetchall()
    # записываем во временные файл новые значения
    import csv
    with open("movement_new.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(record)
    # закрываем соединие
    cursor.close()
    connection.close()

Update_db_dag = DAG('Update_db', start_date=datetime(2022, 10, 17), schedule_interval='*/3 * * * *')

# даг загружает новые значения
Updating_db = PythonOperator(
    python_callable=Update_db,
    dag=Update_db_dag,
    # op_args=[" {{ prev_data_interval_start_success }} "],
    task_id="Update_db"
    # retries = 1,
	# retry_delay =timedelta(minutes=5)
)

Updating_db
