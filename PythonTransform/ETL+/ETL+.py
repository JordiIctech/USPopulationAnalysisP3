import requests
from zipfile import ZipFile

#Beautiful soup
#python data scraping
#python download file from url
#https://www.census.gov/data/datasets/2000/dec/redistricting.html


states = [["Alabama", "al"], 
      ["Alaska", "ak"],    
      ["Arizona", "az"],
      ["Arkansas", "ar"],
      ["California", "ca"],
      ["Colorado", "co"],
      ["Connecticut", "ct"],
      ["Delaware", "de"],
      ["District_of_Columbia", "dc"],
      ["Florida", "fl"],
      ["Georgia", "ga"],
      ["Hawaii", "hi"],
      ["Idaho", "id"],
      ["Illinois", "il"],
      ["Indiana", "in"],
      ["Iowa", "ia"],
      ["Kansas", "ks"],
      ["Kentucky", "ky"],
      ["Louisiana", "la"],
      ["Maine", "me"],
      ["Maryland", "md"],
      ["Massachusetts", "ma"],
      ["Michigan", "mi"],
      ["Minnesota", "mn"],
      ["Mississippi", "ms"],
      ["Missouri", "mo"],
      ["Montana", "mt"],
      ["Nebraska", "ne"],
      ["Nevada", "nv"],
      ["New_Hampshire", "nh"],
      ["New_Jersey", "nj"],
      ["New_Mexico", "nm"],
      ["New_York", "ny"],
      ["North_Carolina", "nc"],
      ["North_Dakota", "nd"],
      ["Ohio", "oh"],
      ["Oklahoma", "ok"],
      ["Oregon", "or"],
      ["Pennsylvania", "pa"],
      ["Puerto_Rico", "pr"],
      ["Rhode_Island", "ri"],
      ["South_Carolina", "sc"],
      ["South_Dakota", "sd"],
      ["Tennessee", "tn"],
      ["Texas", "tx"],
      ["Utah", "ut"],
      ["Vermont", "vt"],
      ["Virginia", "va"],
      ["Washington", "wa"],
      ["West_Virginia", "wv"],
      ["Wisconsin", "wi"],
      ["Wyoming", "wy"]]


for i in states:
    for z in i:
        print(z)


link = "https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/Alabama/al2020.pl.zip"

call = requests.get(link)

open("data/testing.zip", "wb").write(call.content)

with ZipFile('data/testing.zip', 'r') as zipObject:
   listOfFileNames = zipObject.namelist()
   for fileName in listOfFileNames:
       if "geo" in fileName:
           # Extract a single file from zip
           zipObject.extract(fileName, 'data/geo')
           print('Geo Extracted')