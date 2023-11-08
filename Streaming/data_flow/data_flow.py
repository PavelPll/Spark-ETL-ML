# CREATE artificial data flow in HDFS for Spark streaming

import pandas as pd
import numpy as np
import json
import os
from time import sleep
from random import random
from pprint import pprint
import findspark
from time import gmtime, strftime

# ----------------------STATIC DADA preparation--------------------------------
# Create (and save to HDFS) the static dataframe in order to
# be able to convert "num-of-cylinders" from string to int during batch processing 
my_dict = {"num-of-cylinders":['four','six','five','three','twelve','two','eight'], "value":[4,6,5,3,12,2,8]}
DFstatic = pd.DataFrame(my_dict)
DFstatic = DFstatic.astype({'value': 'int32'})

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


command = "hdfs dfs -rm -r /scala/data_flow/*"
print( os.system(command) )
command = "hdfs dfs -rm -r /scala/data_static/*"
print( os.system(command) )
command = "rm data_flow/file*"
print( os.system(command) )
command = "rm data_static/*"
print( os.system(command) )

sleep(10)

with open(f"data_static/data_static.json", "w") as file:
    columns = DFstatic.columns
    for index, row in DFstatic.iterrows():
        li = dict( zip(columns,row) )
        file.write(json.dumps(li) + "\n")

command = "hdfs dfs -put " 
command = command + "./data_static/data_static.json"
command = command + " /scala/data_static/"
os.system(command)

for j in range(400):
    with open(f"data_flow/file_{j}.json", "w") as file:
        rnd = int(round(50*random()*0.1))
        dg = df.sample(n=rnd)
        dg["time"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        dg = dg[["time", "make", "num-of-cylinders", "composed"]]
        print(dg)
        columns = dg.columns
        for index, row in dg.iterrows():
            li = dict( zip(columns,row) )
            file.write(json.dumps(li) + "\n")

    sleep(3)
    command = "hdfs dfs -put " 
    command = command + "./data_flow/file_"+str(j)+".json"
    command = command + " /scala/data_flow/"
    os.system(command)
    sleep(3)
    print(str(j)+" is in hdfs\n")
