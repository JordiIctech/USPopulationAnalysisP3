import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import time
import creds

spark = SparkSession.builder.master("local").appName("Testing") \
.config('fs.s3n.awsAccessKeyId', creds.accesskey) \
.config('fs.s3n.awsSecretAccessKey', creds.secretkey) \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

