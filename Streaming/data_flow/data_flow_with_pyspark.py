# CREATE artificial data flow in HDFS for Spark streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pandas as pd
import numpy as np
import json
import os
from time import sleep
from random import random
from pprint import pprint
import findspark
from time import gmtime, strftime


# Connect to Spark cluster
findspark.init()
spark = SparkSession.builder.master("yarn").appName("Data_Flow").getOrCreate()
# Delete data_flow and data_static from previous run of data_flow_with_pyspark.py
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
fs.delete(spark._jvm.org.apache.hadoop.fs.Path("/scala/data_flow/"), True)
fs.delete(spark._jvm.org.apache.hadoop.fs.Path("/scala/data_static/"), True) 

# ----------------------STATIC DADA preparation--------------------------------
# Create (and save to HDFS) the static dataframe in order to
# be able to convert "num-of-cylinders" from string to int during batch processing 
my_dict = {"num-of-cylinders":['four','six','five','three','twelve','two','eight'], "value":[4,6,5,3,12,2,8]}
DFstatic = pd.DataFrame(my_dict)
DFstatic = DFstatic.astype({'value': 'int32'})
DFstatic_spark = spark.createDataFrame(DFstatic)
DFstatic_spark.coalesce(1).write.json("/scala/data_static/data_static.json")

# ---------------------DYNAMIC DATA preparation -------------------------------
# Read the dataframe from master in order to use it for generation of new data flow (./data_flow) 
# for batch processing
df = pd.read_csv("Automobile_price_data_Raw_.csv", sep=",")
df = df[["make", "fuel-type", "aspiration", "num-of-doors", "body-style", "drive-wheels", "engine-location", "engine-type", "num-of-cylinders", "fuel-system", "price"]]
df = df[["make", "num-of-cylinders", "fuel-system", "price"]]
df = df.replace("?", np.nan).dropna(axis=0)
print(df.dtypes)
df = df.astype({'price': 'int32'})
print(df.dtypes)

df = df.drop_duplicates(subset=["make", "num-of-cylinders", "fuel-system"], keep="first").reset_index(drop=True)
print(len(df))
# Num =  df["num-of-cylinders"].unique()
 
# Merge two columns to unmerge them later during batch processing
df["composed"] = df.apply(lambda x: {"fuel-system":x["fuel-system"], "price":x["price"]}, axis=1)
df = df.drop(["fuel-system"], axis=1).drop(["price"], axis=1)
print(df[:10])
# print(df.sample(n=3))

# Convert Pandas dataframe to Spark dataframe
# in order to get access to HDFS
df_spark = spark.createDataFrame(df)
#df_spark.write.csv("/scala/example.csv")
#df_spark_json = df_spark.toJSON()

# Create "dynamic data" in order to simulate a Data Flow
# the Data Flow is represented by json files, generated from df by parts in a continuous way
# print(df[0:10])

for i in range(20):
    fraction = random() * 0.1
    dg = df_spark.sample(fraction)
    t = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    dg2=dg.withColumn("time", lit(t) )
    dg2 = dg2.select("time", "make", "num-of-cylinders", "composed") 
    dg2.show()
    dg2.coalesce(1).write.json("/scala/data_flow/{}.json".format(i))
    print("File json {} has been genereted in HDFS, waiting several seconds before generating the next one...".format(i))
    sleep(20)


spark.stop()
