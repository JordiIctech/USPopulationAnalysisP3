import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import time
import creds

start_time = time.time()

spark = SparkSession.builder.master("local").appName("Testing") \
.config('fs.s3n.awsAccessKeyId', creds.accesskey) \
.config('fs.s3n.awsSecretAccessKey', creds.secretkey) \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

df0 = spark.read.csv('data/Combine2000RG.csv',header=True)

df1 = df0.select("STUSAB","P0010001")
df1.show()

df0.createOrReplaceTempView("TestT") 

#pyspark.sql("create table mytable as select * from mytempTable")

spark.sql("SELECT STUSAB FROM TestT").show()



#df = spark.read.load('s3://jtechuspopulationp3/predata/Combine2000RG.csv')
#df.show()

print("--- %s seconds ---" % round((time.time() - start_time),4))

