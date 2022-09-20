import sparkC
import time
import pandas as pd
from pyspark.sql import functions as F

start_time = time.time()

spark = sparkC.spark

geoheaders = spark.read.format("csv").option("header", "true").load("data/Geohead.csv")
geoheaders.createOrReplaceTempView("geoheaderimp")

startgeo = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON FROM geoheaderimp")

GeoNames = geoheaders.columns
print(GeoNames)
#Geostring = GeoNames.mkString(",")
#Geolist = Geostring.split(",")


df1 = spark.read.option("delimiter", "|").csv("data/geo2020/flgeo2020.pl")
#df1.show()

geodfhead = df1.toDF(*GeoNames)
geodfhead.createOrReplaceTempView("GeoHeaderakImp")

geodatahead = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, BASENAME, NAME, POP100, INTPTLAT, INTPTLON \
FROM GeoHeaderakImp WHERE SUMLEV = 160 AND NOT (NAME LIKE '%CCD%') AND NOT (NAME LIKE '%township%') AND NOT (NAME LIKE '%(part)%') \
AND NOT (NAME LIKE '%CDP%') AND POP100 > 100000")

geodatahead.show()

#---------------------------------------------------------------------------------
divs = [6, 2, 3, 2, 3, 2, 7, 1, 1, 2, 2, 3, 2, 5, 2, 2, 5, 2, 1, 2, 6, 1, 4, 2, 5, 2, \
2, 4, 5, 2, 1, 3, 5, 2, 5, 2, 4, 2, 2, 1, 4, 4, 1, 2, 1, 5, 2, 1, 1, 2, 2, 2, 2, 3, \
3, 6, 1, 3, 5, 2, 14, 14, 90, 1, 1, 10, 9, 9, 10, 2, 1, 5, 5, 5, 6, 5, 5, 5, 32]
nams = ["FILEID", "STUSAB", "SUMLEV", "GEOCOMP", "CHARITER", "CIFSN", "LOGRECNO", \
"REGION", "DIVISION", "STATECE", "STATE", "COUNTY", "COUNTYSC", "COUSUB", "COUSUBCC", \
"COUSUBSC", "PLACE", "PLACECC", "PLACEDC", "PLACESC", "TRACT", "BLKGRP", "BLOCK", \
"IUC", "CONCIT", "CONCITCC", "CONCITSC", "AIANHH", "AIANHHFP", "AIANHHCC", "AIHHTLI", \
"AITSCE", "AITS", "AITSCC", "ANRC", "ANRCCC", "MSACMSA", "MASC", "CMSA", "MACCI", "PMSA", \
"NECMA", "NECMACCI", "NECMASC", "EXI", "UA", "UASC", "UATYPE", "UR", "CD106", "CD108", \
"CD109", "CD110", "SLDU", "SLDL", "VTD", "VTDI", "ZCTA3", "ZCTA5", "SUBMCDCC", "AREALAND", \
"AREAWATR", "NAME", "FUNCSTAT", "GCUNI", "POP100", "RES", "INTPTLAT", "INTPTLON", "LSADC", \
"PARTFLAG", "SDELM", "SDSEC", "SDUNI", "TAZ", "UGA", "PUMA5", "PUMA1", "RESERVED"]
#print(len(divs))
#print(len(nams))
path = "data/geo2000/flgeo.upl"
df0 = pd.read_fwf(path, widths=divs, names=nams)
print(df0)

df0 = df0.astype(str)
print(df0)

df2=spark.createDataFrame(df0) 
df2.show()

geofhead = df2.select([F.col(col).alias(col.replace(' ', '')) for col in df2.columns])
print("DONE0")
geodfhead.createOrReplaceTempView("GeoHeaderakImp2")
print("DONE1")

geodatahead2 = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, NAME, POP100, INTPTLAT, INTPTLON \
FROM GeoHeaderakImp2 WHERE SUMLEV = 160 AND POP100 > 100000")

"""geodatahead2 = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, NAME, POP100, INTPTLAT, INTPTLON \
FROM GeoHeaderakImp2 WHERE SUMLEV = 160 AND NOT (NAME LIKE '%CCD%') AND NOT (NAME LIKE '%township%') \
AND NOT (NAME LIKE '%(part)%') AND NOT (NAME LIKE '%CDP%') AND POP100 > 100000")"""

geodatahead2.show()

#----------------------------------------------------------------------------------------------------
print("--- %s seconds ---" % round((time.time() - start_time),4))
#====================================================================================================