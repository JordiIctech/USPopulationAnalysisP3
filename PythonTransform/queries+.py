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

#---------------------------------------------------------------------------------
divs = [2, 3, 2, 3, 2, 7, 1, 1, 2, 2, 3, 2, 5, 2, 2, 5, 2, 1, 2, 6, 1, 4, 2, 5, 2, \
2, 4, 5, 2, 1, 3, 5, 2, 5, 2, 4, 2, 2, 1, 4, 4, 1, 2, 1, 5, 2, 1, 1, 2, 2, 2, 2, 3, \
3, 6, 1, 3, 5, 2, 14, 14, 90, 1, 1, 9, 9, 9, 10, 2, 1, 5, 5, 5, 6, 5, 5, 5, 32]
df2 = spark.read.option("delimiter", "  ").csv("data/geo2000/akgeo.upl")
df2.show()

geodfhead2 = df2.toDF(*GeoNames)
geodfhead2.createOrReplaceTempView("GeoHeaderakImp2")

geodatahead2 = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON \
FROM GeoHeaderakImp2 WHERE SUMLEV = 160 AND NOT (NAME LIKE '%CCD%') AND NOT (NAME LIKE '%township%') AND NOT (NAME LIKE '%(part)%') \
AND NOT (NAME LIKE '%CDP%') AND POP100 > 100000")

geodatahead2.show()

#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================