import time
import boto3
import creds

start_time = time.time()

session = boto3.Session(
    aws_access_key_id= creds.accesskey,
    aws_secret_access_key=creds.secretkey
)

s3 = session.resource('s3')

"""
for bucket in s3.buckets.all():
    print(bucket.name)



bucket = s3.Bucket('jtechuspopulationp3')
for obj in bucket.objects.all():
    key = obj.key
    body = obj.get()['Body'].read()
    
    print(key)
    print(body)
    print(str(obj))
"""


#s3 = boto3.resource('s3')
obj = s3.Object("jtechuspopulationp3", "predata/Combine2000RG.csv")
body = obj.get()['Body'].read()
print(body)

print("--- %s seconds ---" % round((time.time() - start_time),4))