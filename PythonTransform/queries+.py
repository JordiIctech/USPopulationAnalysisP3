import sparkC
import time
import pandas

start_time = time.time()

spark = sparkC.spark

geoheaders = spark.read.format("csv").option("header", "true").load("data/Geohead.csv")
geoheaders.createOrReplaceTempView("geoheaderimp")

startgeo = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON FROM geoheaderimp")

GeoNames = geoheaders.columns
print(GeoNames)
#Geostring = GeoNames.mkString(",")
#Geolist = Geostring.split(",")


df1 = spark.read.option("delimiter", "|").csv("data/geo2020/akgeo2020.pl")
df1.show()

geodfhead = df1.toDF(*GeoNames)
geodfhead.createOrReplaceTempView("GeoHeaderakImp")

geodatahead = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON \
FROM GeoHeaderakImp WHERE SUMLEV = 160 AND NOT (NAME LIKE '%CCD%') AND NOT (NAME LIKE '%township%') AND NOT (NAME LIKE '%(part)%') \
AND NOT (NAME LIKE '%CDP%') AND POP100 > 100000")

geodatahead.show()

#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================