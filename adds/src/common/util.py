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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys, traceback
import random
import string
import os
import stat
import errno
import time
from typing import Any, Union
from threading import Thread
from logging import Logger

import boto3
from boto3.dynamodb.conditions import Attr
import json

from common.manifest_dataset import ManifestDataset
from common.bus_dataset import BusDataset
from kafka import  KafkaProducer, KafkaAdminClient 

MAX_ATTEMPTS = 5

def get_s3_client() -> Any:
    """Returns Boto3 S3 client"""

    s3_client = None
    try:
        session = boto3.session.Session()
        s3_client = session.client('s3')
    except Exception as e:
        try:
            print(str(e))
            print(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])
            print(os.environ['AWS_ROLE_ARN'])
            s3_client = boto3.client('s3')
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))

    assert(s3_client != None)
    return s3_client

def random_string(length: int = 64) -> str:
    """Returns a random string of given length with lower and upper case alphanumeric chraracters
    
    Parameters
    ----------
    length: int
        Length of random string

    Returns
    -------
    str
        Random string of given length
    """

    s = ''
    sel = string.ascii_lowercase + string.ascii_uppercase + string.digits
    for _ in range(0, length):
        s += random.choice(sel)
    return s

def is_close_msg(json_msg: dict) -> bool:
    return json_msg.get('__close__', False)

def is_cancel_msg(json_msg: dict) -> bool:
    return json_msg.get('__cancel__', False)

def mkdir_p(path: str):
    try:
        os.makedirs(path)
        os.chmod(path, stat.S_IROTH|stat.S_IWOTH|stat.S_IXOTH|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise

def validate_data_request(request: dict, rosbridge: bool = False):
        
    assert rosbridge or request.get("kafka_topic")

    accept = request.get("accept")
    assert accept.endswith("/rosbag") or accept == "rosmsg" or accept == "manifest"

    if accept.endswith("/rosbag"):
        output = accept.split("/", 1)[0]
        assert output in ["s3", "efs", "fsx"]

    assert request.get("vehicle_id")
    assert request.get("scene_id")
    assert request.get("sensor_id")
    
    assert int(request.get("start_ts")) > 0 
    assert int(request.get("stop_ts")) > int(request.get("start_ts")) 
    assert int(request.get("step")) > 0

    sensors = request.get("sensor_id")
    assert sensors

    if accept.endswith("/rosbag") or accept == "rosmsg":
        ros_topics = request.get("ros_topic")
        assert ros_topics
        data_types = request.get("data_type")
        assert data_types
        
        for s in sensors:
            assert data_types.get(s)
            assert ros_topics.get(s)

def create_manifest(request: dict, 
                    dbconfig: dict, 
                    sensor_id: str, 
                    schema: str) -> Union[BusDataset, ManifestDataset]:

    """Creates a manifest for reading data from Amazon Redshift database
    
    Parameters
    ----------
    request: dict
        Data request
    dbconfig: dict
        Redshift database configuration
    sensor_id: str
        Sesnor id
    schema: str
        Redshift database schema 

    Returns
    -------
    Union[BusDataset, ManifestDataset]
        BusDataset if sensor is bus, else ManifestDataset 
    """

    if sensor_id == 'bus':
        manifest = BusDataset(dbconfig=dbconfig,
                        schema=schema, 
                        vehicle_id=request["vehicle_id"],
                        scene_id=request["scene_id"],
                        start_ts=int(request["start_ts"]), 
                        stop_ts=int(request["stop_ts"]),
                        step=int(request["step"]))
    else:
        manifest = ManifestDataset(dbconfig=dbconfig, 
                        schema=schema,
                        vehicle_id=request["vehicle_id"],
                        scene_id=request["scene_id"],
                        sensor_id=sensor_id,
                        start_ts=int(request["start_ts"]), 
                        stop_ts=int(request["stop_ts"]),
                        step=int(request["step"]))

    return manifest

def load_json_from_file(path: str) -> dict:
    with open(path, "r") as json_file:
        json_data = json.load(json_file)
        return json_data

def delete_kafka_topics(bootstrap_servers: str, kafka_topics: list):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics(kafka_topics)
    admin.close()

def send_kafka_msg(bootstrap_servers: str, kafka_topic: str, kafka_msg: Any):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=random_string())
    producer.send(kafka_topic, json.dumps(kafka_msg).encode('utf-8'))
    producer.flush()
    producer.close()

def s3_bucket_keys(s3_client: Any, bucket_name: str, bucket_prefix: str):
    """Python generator for bucket keys

    Parameters
    ----------
    s3_client: Any
        Boto3 S3 client
    bucket_name: str
        S3 bucket name
    bucket_prefix: str
        S3 bucket prefix
    
    Yields
    -------
    key: str
        Yields bucket keys

    """

    kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get('Contents')
        if contents is None:
            break

        for obj in contents:
            yield obj.get('Key')

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def __retry(attempt: int) -> int:
    if attempt <= MAX_ATTEMPTS:
        attempt += 1

    interval = random.uniform(5**(attempt - 1), 5**attempt)
    time.sleep(interval)
    return attempt

def download_s3_directory(s3_client: Any, bucket: str, bucket_prefix: str, local_path: str, logger: Logger = None):
    """Download S3 directory recursively

    Parameters
    ----------
    s3_client: Any
        Boto3 S3 client
    bucket: str
        S3 bucket name
    bucket_path: str
        S3 bucket path to upload file
    local_path: str
        Local path for downloaded directory
    logger; Logger
        Logger for logging error messages
    """

    attempt = 0
    prefix_len = len(bucket_prefix) + 1
    while True:
        try:
            for key in s3_bucket_keys(s3_client, bucket_name=bucket, bucket_prefix=bucket_prefix):
                local_file_path = os.path.join(local_path, key[prefix_len:])
                local_dir_path = os.path.dirname(local_file_path)
                mkdir_p(local_dir_path)
                s3_client.download_file(bucket, key, local_file_path) 
            break
        except Exception:
            attempt = __retry(attempt=attempt)
            if logger:
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(str(exc_type))
                logger.warning(str(exc_value))

def download_s3_file(s3_client: Any, bucket: str, key: str, local_path: str, logger: Logger = None):
    """Download S3 file with automatic retry for failures

    Parameters
    ----------
    s3_client: Any
        Boto3 S3 client
    bucket: str
        S3 bucket name
    bucket_path: str
        S3 bucket path to upload file
    local_path: str
        Local path for downloaded file
    logger; Logger
        Logger for logging
    """

    attempt = 0
    while True:
        try:
            s3_client.download_file(bucket, key, local_path)
            break
        except Exception:
            attempt = __retry(attempt=attempt)
            if logger:
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(str(exc_type))
                logger.warning(str(exc_value))

def delete_s3(s3_client: Any, bucket: str, bucket_path: str, logger: Logger = None):
    """Delete S3 object

    Parameters
    ----------
    s3_client: Any
        Boto3 S3 client
    bucket: str
        S3 bucket name
    bucket_path: str
        S3 bucket path to upload file
    logger; Logger
        Logger for logging
    """

    attempt = 0
    while True:
        try:
            for key in s3_bucket_keys(s3_client, bucket_name=bucket, bucket_prefix=bucket_path):
                s3_client.delete_object(Bucket=bucket, Key=key)
            break
        except Exception:
            attempt = __retry(attempt=attempt)
            if logger:
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(str(exc_type))
                logger.warning(str(exc_value))

def upload_s3(s3_client: Any, local_path: str, bucket: str, bucket_path: str, logger: Logger = None):
    """Upload local file to S3 bucket

    Parameters
    ----------
    s3_client: Any
        Boto3 S3 client
    local_path: str
        Local path of file
    bucket: str
        S3 bucket name
    bucket_path: str
        S3 bucket path to upload file
    logger; Logger
        Logger for logging
    """
    
    attempt = 0
    while True:
        try:
            if os.path.isdir(local_path):
                for root, _ ,files in os.walk(local_path):
                    for file in files:
                        key = os.path.join(bucket_path, file)
                        local_file_path = os.path.join(root, file)
                        s3_client.upload_file(local_file_path, bucket, key)
            elif os.path.isfile(local_path):
                s3_client.upload_file(local_path, bucket, bucket_path)
            break
        except Exception:
            attempt = __retry(attempt=attempt)
            if logger:
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(str(exc_type))
                logger.warning(str(exc_value))

def get_data_requests( data_request_table: Any, request_hash: Any, ros_version: Any):
    """A Python generator for yielding data requests cached in an Amazon Dynamodb table

    Parameters
    ----------
    data_request_table: Any
        Amazon Dynamodb table boto3 resource
    
    request_hash: Any
        Data request hash

    ros_version: Any
        Ros version
    
    Yields
    -------
    Yields cached data requests 
    """

    response = None
    exclusive_start_key = None
    filter_expression = Attr('request_hash').eq(request_hash) & Attr('ros_version').eq(ros_version)
    
    while exclusive_start_key is not None or response is None:
       
        if exclusive_start_key:
            response = data_request_table.scan(
                Select='SPECIFIC_ATTRIBUTES',
                ProjectionExpression='request_index,request_json,request_location',
                FilterExpression=filter_expression,
                ExclusiveStartKey=exclusive_start_key)
        else:
            response = data_request_table.scan(
                Select='SPECIFIC_ATTRIBUTES',
                ProjectionExpression='request_index,request_json,request_location',
                FilterExpression=filter_expression)

        exclusive_start_key = response.get('LastEvaluatedKey', None)
        yield response.get('Items')

    

