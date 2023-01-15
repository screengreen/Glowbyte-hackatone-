from pyspark.sql import SparkSession
# import pyspark.sql.types as t
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

import pandas

spark = SparkSession.builder.getOrCreate()
schema1 = StructType([\
     StructField("driver_license", StringType(), True),\
     StructField("first_name", StringType(), True),\
     StructField("last_name", StringType(), True),\
     StructField("middle_name", StringType(), True), \
     StructField("driver_valid_to", DateType(), True), \
     StructField("card_num", StringType(), True), \
     StructField("update_dt", DateType(), True), \
     StructField("birth_dt", DateType(), True), \
         ])


df = (
        spark
        .read
        .format("csv")
        .schema(schema1)
        # .option('header', 'true')
        .load('/Users/andreisuhov/PycharmProjects/HackatonGlow/drivers.csv')
)



df.select("last_name", "first_name", "middle_name","birth_dt", "card_num","driver_license","driver_valid_to" )
df.createOrReplaceTempView('df')

test_df = ( spark
            .sql("""  Select row_number() over (order by last_name) as num, 0, last_name, first_name, middle_name, birth_dt, card_num,driver_license, IF(DATEDIFF(DAY,DATE(NOW()),driver_valid_to) > 0, TRUE, FALSE) as date from df """)
            # .withColumn("id", monotonically_increasing_id())
            .withColumn('deleted_flag', lit(0))
            .withColumn('end_dt', lit(0))
            )

test_df.write.mode('overwrite').option("header", "true").format('csv').save('/Users/andreisuhov/PycharmProjects/HackatonGlow/result.csv')

# import pyspark.sql.functions as F
# NAME = 'Audi'
# df.select(F.col("manufacturer_name"), F.col("model_name")).show(5)
# df\
#     .select("manufacturer_name", "model_name", "transmission")\
#     .filter(F.col("manufacturer_name") == NAME)\
#     .filter(" transmission = 'mechanical'").show(5)
#
# ds = df.select("manufacturer_name").distinct().count()
# print(ds)
#
# df.groupBy('manufacturer_name').count().orderBy(F.col('count').desc()).show()
# df.withColumnRenamed('manufacturer_name', 'manufacturer').show(5)
#
# df.withColumn('next_year', F.col("year_produced")+1).select("year_produced", "next_year").show(5)
#
# df.printSchema()
# df.select('year_produced').describe().show(5)
spark.stop()
# #
# # def main():
# #     spark = SparkSession.builder.getOrCreate()
# #     df = spark.read.format('csv').option('header', 'true').load(
# #         '/Users/andreisuhov/PycharmProjects/Sparkbitch/practice-2/data/cars.csv')
# #
# #     output = (
# #         df
# #         .groupBy("manufacturer_name")
# #         .agg(
# #             F.count("manufacturer_name").alias('count'),
# #             F.round(F.avg("year_produced")).cast(t.IntegerType()).alias('avg_year'),
# #             F.max('price_usd').alias('max_price'),
# #             F.min('price_usd').alias('min_price')
# #         )
# #     )
# #     output.write.mode('overwrite').format('json').save('result.json')
# # main()