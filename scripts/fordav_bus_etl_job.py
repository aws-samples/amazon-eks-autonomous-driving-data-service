'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

from typing import Any
import boto3
import time
import json

def s3_bucket_keys(s3_client: Any, bucket_name: str, bucket_prefix: str):
    """Generator for listing S3 bucket keys matching prefix"""

    kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            yield obj['Key']

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def s3_delete_prefix(s3_client: Any, bucket_name: str, bucket_prefix: str):
    try:
        for key in s3_bucket_keys(s3_client, bucket_name, bucket_prefix):
            s3_client.delete_object(Bucket=bucket_name,Key=key)
    except KeyError:
        pass


def athena_query(client: Any, params: dict) -> Any:
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_sync(client: Any, params: dict):
    execution = athena_query(client=client, params=params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while ( state in ['RUNNING', 'QUEUED']):
        response = client.get_query_execution(QueryExecutionId = execution_id)
        print(f"{response}")
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                raise RuntimeError(str(response))
            elif state == 'SUCCEEDED':
                return  response['QueryExecution']['ResultConfiguration']['OutputLocation']
        time.sleep(2)

def create_imu_table(athena_client: Any, database: str, storage_location: str, output_bucket: str):
    params = dict()
    params['database'] = database
    params['query'] = f"""
        CREATE EXTERNAL TABLE `imu`(
            `header` struct<seq:int,stamp:struct<secs:int,nsecs:int>,frame_id:string> COMMENT 'from deserializer', 
            `orientation` struct<x:double,y:double,z:double,w:double> COMMENT 'from deserializer', 
            `orientation_covariance` array<double> COMMENT 'from deserializer', 
            `angular_velocity` struct<x:double,y:double,z:double> COMMENT 'from deserializer', 
            `angular_velocity_covariance` array<double> COMMENT 'from deserializer', 
            `linear_acceleration` struct<x:double,y:double,z:double> COMMENT 'from deserializer', 
            `linear_acceleration_covariance` array<double> COMMENT 'from deserializer')
            PARTITIONED BY ( 
            `vehicle_id` string, 
            `scene_id` string)
            ROW FORMAT SERDE 
            'org.openx.data.jsonserde.JsonSerDe' 
            WITH SERDEPROPERTIES ( 
            'paths'='angular_velocity,angular_velocity_covariance,header,linear_acceleration,linear_acceleration_covariance,orientation,orientation_covariance') 
            STORED AS INPUTFORMAT 
            'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
            '{storage_location}'
            TBLPROPERTIES (
            'classification'='json', 
            'compressionType'='none',  
            'typeOfData'='file')
        """
    params['bucket'] = output_bucket
    params['path'] = f"athena/output/"

    athena_sync(client=athena_client, params=params)

    params['query'] = "MSCK REPAIR TABLE imu"
    athena_sync(client=athena_client, params=params)

def create_gps_table(athena_client: Any, database: str, storage_location: str, output_bucket: str):
    params = dict()
    params['database'] = database
    params['query'] = f"""
        CREATE EXTERNAL TABLE `gps`(
            `header` struct<seq:int,stamp:struct<secs:int,nsecs:int>,frame_id:string> COMMENT 'from deserializer', 
            `status` struct<status:int,service:int> COMMENT 'from deserializer', 
            `latitude` double COMMENT 'from deserializer', 
            `longitude` double COMMENT 'from deserializer', 
            `altitude` double COMMENT 'from deserializer', 
            `position_covariance` array<double> COMMENT 'from deserializer', 
            `position_covariance_type` int COMMENT 'from deserializer')
            PARTITIONED BY ( 
            `vehicle_id` string, 
            `scene_id` string)
            ROW FORMAT SERDE 
            'org.openx.data.jsonserde.JsonSerDe' 
            WITH SERDEPROPERTIES ( 
            'paths'='altitude,header,latitude,longitude,position_covariance,position_covariance_type,status') 
            STORED AS INPUTFORMAT 
            'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
            '{storage_location}'
            TBLPROPERTIES ( 
            'classification'='json', 
            'compressionType'='none',
            'typeOfData'='file')
        """
    params['bucket'] = output_bucket
    params['path'] = f"athena/output/"

    athena_sync(client=athena_client, params=params)

    params['query'] = "MSCK REPAIR TABLE gps"
    athena_sync(client=athena_client, params=params)

def main(config: dict):

    s3_client = boto3.client(service_name='s3')

    s3_bucket = config["s3_bucket"]
    # delete existing output prefix in case we there is a retry attempt
    s3_delete_prefix(s3_client, bucket_name=s3_bucket, bucket_prefix=config["s3_output_prefix"] )
   
    glue = boto3.client(service_name='glue')
    database = config['database']
    glue.create_database( DatabaseInput={'Name': database})

    s3_storage_prefix = config["s3_storage_prefix"]
    athena_client = boto3.client("athena")
    storage_location = f"s3://{s3_bucket}/{s3_storage_prefix}/imu/"
    create_imu_table(athena_client=athena_client, database=database,
                    storage_location=storage_location, output_bucket=s3_bucket)
    
    storage_location = f"s3://{s3_bucket}/{s3_storage_prefix}/gps/"
    create_gps_table(athena_client=athena_client, database=database,
                    storage_location=storage_location, output_bucket=s3_bucket)

    job_name=f"fordav-bus-etl-{str(time.time()).replace('.','')}"

    job = glue.create_job(Name=job_name, Role=config["glue_role"],
            GlueVersion='2.0',
            WorkerType='G.2X',
            NumberOfWorkers=11,
            Command={'Name': 'glueetl',
                    'ScriptLocation': config['script_location'],
                    'PythonVersion': '3'
                    }, 
            DefaultArguments = {'--job-language': 'python',
                                '--database': database,
                                '--s3_bucket':   config["s3_bucket"],
                                '--s3_output_prefix': config["s3_output_prefix"]})

    job_run = glue.start_job_run(JobName=job['Name'])
    status = glue.get_job_run(JobName=job['Name'], RunId=job_run['JobRunId'])
    print(str(status))

    run_state = status['JobRun']['JobRunState']
    while run_state == "RUNNING" or run_state == "STARTING" or run_state == "STOPPING" or run_state == "STOPPED":
        print(f"Glue Job is {run_state}")
        time.sleep(30)
        status = glue.get_job_run(JobName=job['Name'], RunId=job_run['JobRunId'])
        run_state = status['JobRun']['JobRunState']
    
    glue.delete_database( Name=database)
    if run_state != "SUCCEEDED":
        import sys
        sys.exit(f"Glue job final status: {run_state}")

    

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Fordav Bus ETL Job')
    parser.add_argument('--config', type=str,  help='Fordav Bus ETL Configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config)

