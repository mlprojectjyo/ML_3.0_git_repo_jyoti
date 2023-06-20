import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl-car-company [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("EMRSparkETLCarCompany")\
        .getOrCreate()

    CarCompany = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1])

    updatedCarCompany = CarCompany.withColumn("current_date", lit(datetime.now()))

    updatedCarCompany.printSchema()

    print(updatedCarCompany.show())

    print("Total number of records: " + str(updatedCarCompany.count()))

    updatedCarCompany.write.parquet(sys.argv[2])
