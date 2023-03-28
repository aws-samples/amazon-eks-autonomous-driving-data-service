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

import csv
import os
import random
import tempfile
import time
import json
import sys
import re
from typing import Any
from uuid import uuid4

import boto3
import logging


class FordavDriveData:

    PREFIXES =[ 
        "camera_center",
        "camera_front_left",
        "camera_front_right",
        "camera_rear_left",
        "camera_rear_right",
        "camera_side_left",
        "camera_side_right",
        "gps",
        "gps_time",
        "imu",
        "lidar_blue_pointcloud",
        "lidar_green_pointcloud",
        "lidar_red_pointcloud",
        "lidar_yellow_pointcloud",
        "pose_ground_truth",
        "pose_localized",
        "pose_raw",
        "tf",
        "velocity_raw"
    ]

    def __init__(self, config:dict):  
        logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.__logger = logging.getLogger("fordav-drive-data")
        self.__config = config
        self.__s3_client = boto3.client("s3")
        self.__batch_client = boto3.client("batch")

    def __submit_job(self, job_name: str, command: list) -> Any:
        job_queue = self.__config["job_queue"]
        job_definition = self.__config["job_definition"]
        
        return self.__batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            retryStrategy={'attempts': 1},
            timeout={'attemptDurationSeconds': 36000},
            containerOverrides={
                'command': command,
                'environment': [
                    {
                        'name': 'S3_PYTHON_SCRIPT',
                        'value': os.environ["S3_PYTHON_SCRIPT"]
                    },
                    {
                        'name': 'S3_JSON_CONFIG',
                        'value': os.environ["S3_JSON_CONFIG"]
                    },
                    {
                        'name': 'AWS_DEFAULT_REGION',
                        'value': os.environ['AWS_DEFAULT_REGION']
                    }
                ]
            })
    
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

    def __submit_drive_data_jobs(self):
        dest_prefix = self.__config["dest_prefix"]
        
        # get a list of objects in the source bucket
        self.__jobs=dict()
        for prefix in self.PREFIXES:
            data_prefix = f"{dest_prefix}/{prefix}/"
            command = ["--data-prefix", data_prefix]
            job_name = f"fordav-{prefix}-{str(time.time()).replace('.','-')}"

            response = self.__submit_job(job_name=job_name, command=command)
            jobId = response["jobId"]
            self.__jobs[jobId] = job_name

        self.__wait_for_jobs()

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

    @classmethod
    def __retry(cls, attempt:int) -> int:
        if attempt <= cls.MAX_ATTEMPTS:
            attempt += 1
        
        interval = random.uniform(2**(attempt - 1), 2**attempt)
        cls.logger.info(f"Will retry after: {interval} seconds")
        time.sleep(interval)
        return attempt
    
    def __s3_upload_file(self, local_path:str, bucket:str, key:str):
        """Upload local file to S3 bucket
        """
        attempt = 0
        while True:
            try:
                self.__s3_client.upload_file(local_path, bucket, key)
                break
            except Exception as error:
                self.__logger.warning(f"Upload file error: {local_path} -> {key}, {error}")
                attempt = self.__retry(attempt=attempt)

    def __load_drive_data(self):
        with tempfile.NamedTemporaryFile(mode='w',  newline='', suffix=".csv") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',')
            dest_bucket = self.__config['dest_bucket']
            data_prefix = self.__config['data_prefix']
            
            nrows = 0
            self.__logger.info(f"Load drive data for: {data_prefix}")
            for key in self.__s3_bucket_keys( bucket_name=dest_bucket, bucket_prefix=data_prefix):
                m=re.match(".+\/(\w+)\/vehicle_id=(\w+)\/scene_id=(.+)\/(\d+)\.(pcd|png|json)", key)
                if m:
                    row = [m[2], m[3], m[1], m[4], dest_bucket, key]
                    csvwriter.writerow(row)
                    nrows += 1

                    if nrows % 1000 == 0:
                        self.__logger.info(f"Drive data row count: {nrows}")

            self.__logger.info(f"Drive data row count: {nrows}")
            drive_data_prefix = self.__config.get('drive_data_prefix')
            drive_data_key = f"{drive_data_prefix}/{str(uuid4())}.csv"
            self.__logger.info(f"Upload drive data: {drive_data_key}")
            self.__s3_upload_file(local_path=csvfile.name, bucket=dest_bucket, key=drive_data_key)

    def __call__(self):

        if not self.__config['data_prefix']:
            self.__submit_drive_data_jobs()
        else:
            self.__load_drive_data()

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load fordav drive data')
    parser.add_argument('--config', type=str,  help='Configuration JSON file', required=True)
    parser.add_argument('--data-prefix', type=str,  default='', help="Data prefix", required=False)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    config['data_prefix'] = args.data_prefix
    fordav_drive_data = FordavDriveData(config=config)
    fordav_drive_data()

    sys.exit()
