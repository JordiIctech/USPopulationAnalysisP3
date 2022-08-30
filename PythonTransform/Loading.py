#Other Project Files
import awsC
import sparkC
import creds
# Modules
import time
import pandas as pd
from smart_open import smart_open
#====================================================================================================
start_time = time.time()
#----------------------------------------------------------------------------------------------------
#Set-Up
spark = sparkC.spark

s3 = awsC.s3

path = 's3://{}:{}@{}/{}'.format(creds.accesskey, creds.secretkey, "jtechuspopulationp3", "predata/Combine2010RG.csv")

df0 = pd.read_csv(smart_open(path))
print(df0)

df1=spark.createDataFrame(df0) 
df1.show()

df2 = df1.select("STUSAB","P0010001")

df2.createOrReplaceTempView("TestT2") 

spark.sql("SELECT STUSAB FROM TestT2").show()
#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================

#Current thread: https://stackoverflow.com/questions/38867472/spark-select-where-or-filtering