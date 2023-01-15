from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as t
import psycopg2
# from functions import Connect_and_collect_db

def Connect_and_collect_db():
    # try:
    connection = psycopg2.connect(user="etl_tech_user",
                                  database="taxi",
                                  password="etl_tech_user_password",
                                  host="de-edu-db.chronosavant.ru",
                                  port="5432")

    cursor = connection.cursor()
    import csv

    # 1
    # cursor.execute('SELECT * FROM "main"."car_pool" ')
    # record = cursor.fetchall()
    # with open("car_pool.csv", "w", newline="") as f:
    #     writer = csv.writer(f)
    #     writer.writerows(record)

    # 2
    # cursor.execute('SELECT * FROM "main"."drivers" ')
    # record = cursor.fetchall()
    # with open("drivers.csv", "w", newline="") as f:
    #     writer = csv.writer(f)
    #     writer.writerows(record)

    # 3
    cursor.execute('SELECT * FROM "main"."movement"  ')
    record = cursor.fetchall()
    with open("movement_new.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(record)

    # 4
    # cursor.execute('SELECT * FROM "main"."rides" ')
    # record = cursor.fetchall()
    # with open("rides.csv", "w", newline="") as f:
    #     writer = csv.writer(f)
    #     writer.writerows(record)

    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")
    # except :
    #     print("Ошибка при работе с PostgreSQL111111111")


Connect_and_collect_dag = DAG('Download_full_db', start_date=days_ago(0,0,0,0,0))


Connecting_to_db = PythonOperator(
    python_callable=Connect_and_collect_db,
    dag=Connect_and_collect_dag,
    task_id="Connect_and_collect_db"
)


Connect_and_collect_dag



