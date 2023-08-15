import pandas as pd
import os
import boto3, os, io
from io import BytesIO
import warnings
warnings.filterwarnings("ignore")

bucket_name = 'weather-innova-proto'



def getS3Object(s3Client,s3Bucket,filepath):
    f = s3Client.get_object(Bucket=s3Bucket, Key=filepath) 
    return pd.read_csv(io.BytesIO(f['Body'].read()), header=0,index_col=False) 
def getAllS3Objects(s3key,s3Secret,s3Bucket):
    session = boto3.Session(aws_access_key_id=s3key,aws_secret_access_key=s3Secret) 
    s3Client = session.client('s3') 
    response = s3Client.list_objects(Bucket=s3Bucket).get('Contents', [])
    col = ["fecha","hour","condition","temp_C","will_rain","chance_of_rain"]
    df = pd.DataFrame(columns = col)
    for content in response:
        file = content.get('Key', [])
        df = df.append(getS3Object(s3Client,s3Bucket,file),ignore_index=True)

    return df

df = getAllS3Objects(my_key,my_secret,bucket_name)




