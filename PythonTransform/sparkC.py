import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import time

start_time = time.time()

spark = SparkSession.builder.master("local").appName("Testing").config("spark.some.config.option", "some-value").getOrCreate()

df0 = spark.read.csv('data/Combine2000RG.csv',header=True)

df1 = df0.select("STUSAB","P0010001")
df1.show()

df0.createOrReplaceTempView("TestT") 

#pyspark.sql("create table mytable as select * from mytempTable")

spark.sql("SELECT STUSAB FROM TestT").show()


print("--- %s seconds ---" % round((time.time() - start_time),4))

