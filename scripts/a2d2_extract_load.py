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

import boto3
import os
import time
import json
import logging
import sys

class A2d2ExtractLoad(object):
    GB = 1024**3
    MB = 1024**2
    S3_MAX_IO_QUEUE = 1000
    S3_IO_CHUNKSIZE = 262144
    MAX_ATTEMPTS = 4
     
    def __init__(self, config:dict):
        super().__init__()
        
        logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.__logger = logging.getLogger("a2d2-extract-load")
        self.__config = config
        self.__s3_client = boto3.client("s3")
        self.__batch_client = boto3.client("batch")

    def __s3_bucket_keys(self, bucket_name:str, bucket_prefix:str):
        """Generator for listing S3 bucket keys matching prefix"""

        kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
        while True:
            resp = self.__s3_client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                yield obj['Key']

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def __stagger_jobs(self, interval:float):
        self.__logger.info(f"Waiting for {interval} secs between jobs")
        time.sleep(interval)

    def __submit_tar_jobs(self, keys:list):
        job_queue = self.__config["job_queue"]
        job_definition = self.__config["job_definition"]
        s3_python_script = self.__config["s3_python_script"]
        s3_json_config = self.__config["s3_json_config"]

        aws_region = os.environ['AWS_DEFAULT_REGION']

        for key, interval in keys:
            job_name = str(time.time()).replace('.','-')
            response = self.__batch_client.submit_job(
                jobName=f'extract-tar-{job_name}',
                jobQueue=job_queue,
                jobDefinition=job_definition,
                retryStrategy={'attempts': 5},
                timeout={'attemptDurationSeconds': 86400},
                containerOverrides={
                    'command': ['--key', f'{key}'],
                    'environment': [
                        {
                            'name': 'S3_PYTHON_SCRIPT',
                            'value': s3_python_script
                        },
                        {
                            'name': 'S3_JSON_CONFIG',
                            'value': s3_json_config
                        },
                        {
                            'name': 'AWS_DEFAULT_REGION',
                            'value': aws_region
                        }
                    ]
                })
                
            jobId = response["jobId"]
            self.__jobs[jobId] = job_name
            self.__stagger_jobs(interval=interval)

    def __call__(self):
        source_bucket = self.__config["source_bucket"]
        source_prefix = self.__config["source_prefix"]
        dest_bucket = self.__config["dest_bucket"]
        dest_prefix = self.__config["dest_prefix"]
       
        self.__jobs=dict()

        # get a list of objects in the source bucket
        tar_keys = []

        for key in self.__s3_bucket_keys(bucket_name=source_bucket, bucket_prefix=source_prefix):
            if key.find(".tar") == -1:
                self.__s3_client.copy_object(Bucket=dest_bucket, Key=f'{dest_prefix}/{key}',
                    CopySource = {'Bucket': source_bucket, 'Key': key})
            else:
                file_size = self.__s3_client.head_object(Bucket=source_bucket, Key=key).get('ContentLength')
                interval = file_size/(100*self.MB)
                tar_keys.append((key, interval))

        tar_keys.sort(key=lambda tup: tup[1], reverse=True)
        self.__submit_tar_jobs(keys=tar_keys)
        self.__wait_for_jobs()

    def __wait_for_jobs(self):
        self.__pending=[ job_id for job_id in self.__jobs.keys() ]

        while self.__pending:
            pending_jobs = []
            for i in range(0, len(self.__pending), 100):

                jobs_slice = self.__pending[i:i+100]
                if jobs_slice:
                    response = self.__batch_client.describe_jobs(jobs=jobs_slice)
                    
                    for _job in response["jobs"]:
                        job_id = _job['jobId']
                        if _job["status"] == 'FAILED':
                            reason = f'Job failed: {job_id}'
                            self.__abort(reason)
                        elif _job['status'] != 'SUCCEEDED':
                            pending_jobs.append(job_id)
            
            self.__pending = pending_jobs

            time.sleep(60)
    
    def __abort(self, reason):
        for job_id in self.__jobs.keys():
            try:
                self.__batch_client.terminate_job(jobId=job_id, reason=reason)
            except Exception as e:
                self.__logger.warning(f"ignoring {e}")

        sys.exit(reason)
    
    
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A2d2 Extract and load data to S3')
    parser.add_argument('--config', type=str,  help='configuration JSON file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    a2d2_extract_load = A2d2ExtractLoad(config=config)
    a2d2_extract_load()