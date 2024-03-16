import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
   
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    sns_arn = 'arn:aws:sns:ap-south-1:767397944091:DoorDashDeliveredStatus-SUCCESS'
    response = s3_client.get_object(Bucket=bucket_name,Key= key)
    
    # response will be in byte type, we have to read the byte type format and decode it to convert the byte type into user readable format
    json_content = response['Body'].read().decode('utf-8')

    # got trailing data error becasue the file had multiple JSON objects separated by whitespaces or any characters and not formatted as array. 
    # In this case, it was whitespace
    # so looping through the content and appending each line in a list
    json_lines = []
    for line in json_content.splitlines():
        json_lines.append(line)
    
    #concat the json_lines list into an array with ',' to remove whitespaces
    concat_json = '[' + ','.join(json_lines) + ']'
    
    # parsing the byte type format to String  
    df = pd.read_json(StringIO(concat_json))
    
    #filtering the df to get only the delivered data
    df = df[df['status'] == 'delivered']
    
    # converting the df to json 
    delivered_json_data = df.to_json(orient = 'records')
    
    current_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    target_bucket_name = 'doordash-target-zone-59544'
    target_key = f'delivered_data_{current_date_time}.json'
    
    try:
        s3_client.put_object(Bucket = target_bucket_name, Key = target_key, Body = delivered_json_data)
        print(f"{target_key} uploaded successfully in {target_bucket_name}")
        message = f'SUCCESS! {key} file has been processed successfully and {target_key} has been uploaded in {target_bucket_name} bucket'
        mail = sns_client.publish(Subject='SUCCESS!', TargetArn = sns_arn, Message = message, MessageStructure = 'text')
        print("Success mail sent successfully")
    except Exception as e:
        print("Error while uploading the file:", e)
        message = f'FAILURE! {key} file processing failed'
        mail = sns_client.publish(Subject='FAILURE!', TargetArn = sns_arn, Message = message, MessageStructure = 'text')
        print("Failure mail sent successfully")
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('files uploaded in s3 bucket has been processed and delivered to the  target bucket')
    }
