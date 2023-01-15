from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as t


def Connect_and_collect_db():
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user="etl_tech_user",
                                      database="taxi",
                                      password="etl_tech_user_password",
                                      host="de-edu-db.chronosavant.ru",
                                      port="5432")

        cursor = connection.cursor()
        import csv

        # 1
        cursor.execute('SELECT * FROM "main"."car_pool" ')
        record = cursor.fetchall()
        with open("/Users/andreisuhov/PycharmProjects/HackatonGlow/car_pool.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(record)

        # 2
        cursor.execute('SELECT * FROM "main"."drivers" ')
        record = cursor.fetchall()
        with open("drivers.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(record)

        # 3
        cursor.execute('SELECT * FROM "main"."movement" ')
        record = cursor.fetchall()
        with open("movement.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(record)

        # 4
        cursor.execute('SELECT * FROM "main"."rides" ')
        record = cursor.fetchall()
        with open("rides.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(record)

        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

def download_titanic_dataset():
    spark = (
        SparkSession
            .builder
            .getOrCreate()
    )

    df = (
        spark
            .read
            .format("csv")
            .option('header', 'true')
            .load(reading_file)
    )

    df.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv("/Users/andreisuhov/PycharmProjects/HackatonGlow/files")

    # df = pd.read_csv("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
    # df.to_csv("df.csv")

def transform_dataset():
    spark = (
        SparkSession
            .builder
            .getOrCreate()
    )

    df = (
        spark
            .read
            .format("csv")
            .option('header', 'true')
            .load(reading_file)
    )

    df2 = df.groupBy('manufacturer_name').count().orderBy(F.col('count').desc())
    df2.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").format("csv").save("/Users/andreisuhov/PycharmProjects/HackatonGlow/files2")

    # df = pd.read_csv('df.csv')
    # del df['Unnamed: 0']
    # gr = df.groupby('Pclass').agg({'Survied': "mean"})
    # gr.to_csv("gr.csv")
    # return df