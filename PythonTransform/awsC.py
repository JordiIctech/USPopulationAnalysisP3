import time
import boto3
import creds
import sparkC
import pandas as pd
from io import StringIO

start_time = time.time()

session = boto3.Session(
    aws_access_key_id= creds.accesskey,
    aws_secret_access_key=creds.secretkey
)

spark = sparkC.spark   #Loads and runs sparkC file (imported first)

s3 = session.resource('s3')

obj = s3.Object("jtechuspopulationp3", "predata/Combine2010RG.csv")
body = obj.get()['Body'].read()
#print(body)

# ---------------------------------------------------------------------------------------------
from smart_open import smart_open

path = 's3://{}:{}@{}/{}'.format(creds.accesskey, creds.secretkey, "jtechuspopulationp3", "predata/Combine2010RG.csv")

df0 = pd.read_csv(smart_open(path))
print(df0)

df1=spark.createDataFrame(df0) 
df1.show()

df2 = df1.select("STUSAB","P0010001")

df2.createOrReplaceTempView("TestT2") 

spark.sql("SELECT STUSAB FROM TestT2").show()

print("--- %s seconds ---" % round((time.time() - start_time),4))

#Current thread: https://stackoverflow.com/questions/38867472/spark-select-where-or-filtering