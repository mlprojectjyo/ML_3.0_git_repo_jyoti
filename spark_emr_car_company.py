#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import the necessary modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# # Load data

# In[12]:


# Create a SparkSession & load data
# file_loc = "/home/ec2-user/SageMaker/ML_3.0_git_repo_jyoti/Car_details.csv"
file_loc = "s3://ml-3-s3-bucket-jyoti/rawdata/Car_details.csv"
file_type = "csv"

infer_schema = "true"
first_row_header = "true"
delimiter = ","

spark = SparkSession.builder.appName('spark_car_company').getOrCreate()

spark_df = spark.read.format(file_type) \
     .option("inferSchema", infer_schema) \
     .option("header", first_row_header) \
     .option("sep", delimiter) \
     .load(file_loc)


# # Exploratory Analysis

# In[3]:


spark_df.printSchema()


# 

# In[ ]:





# In[4]:


display(spark_df)


# In[5]:


spark_df.count()


# # Check for Duplicates

# In[6]:


df_duplicates = spark_df.groupBy(spark_df.columns).count().filter("count > 1")
df_duplicates.show()


# #### There are no duplicates as we can see the dataframe returned is empty

# # Checking for Nulls

# In[7]:


# checking null/na in all the columns
nulls_df = {col:spark_df.filter(spark_df[col].isNull()).count() for col in spark_df.columns}
nulls_df


# 

# 

# In[ ]:





# 

# ## Calculating selling age of the car from year column

# In[8]:


#Validation
spark_df.createOrReplaceTempView("df_year")
df_year_diff=spark.sql("select *, 2023-year as age_car from df_year")
df_year_diff.show()


# ##  Calculating no of owners from owner column

# In[9]:


df_year_diff = df_year_diff.withColumn("owner_cln", when(df_year_diff["owner"] == "First Owner", 1)
                                .when(df_year_diff["owner"] == "Second Owner", 2)
                                .when(df_year_diff["owner"] == "Third Owner", 3)
                                .when(df_year_diff["owner"] == "Fourth & Above Owner", 4)
                                .otherwise(5))

df_year_diff.show()


# 

# In[ ]:





# In[13]:


# df_year_diff.write.mode('overwrite').parquet("s3://ml-3-s3-bucket-jyoti/curateddata/spark/car_analysis.parquet")
df_year_diff.write.mode('overwrite').parquet("/home/ec2-user/SageMaker/ML_3.0_git_repo_jyoti/car_analysis.parquet")


# In[ ]:




