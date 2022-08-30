import time
import boto3
import creds
import sparkC
import pandas as pd
from io import StringIO

session = boto3.Session(
    aws_access_key_id= creds.accesskey,
    aws_secret_access_key=creds.secretkey
)

s3 = session.resource('s3')

#Current thread: https://stackoverflow.com/questions/38867472/spark-select-where-or-filtering