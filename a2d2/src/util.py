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

import boto3
import json

from manifest_dataset import ManifestDataset
from bus_dataset import BusDataset
from kafka import  KafkaProducer, KafkaAdminClient
from concurrent.futures import ThreadPoolExecutor # Workaround for https://github.com/boto/boto3/issues/3221 

def get_s3_client():

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

def random_string(length=16):
    s = ''
    sel = string.ascii_lowercase + string.ascii_uppercase + string.digits
    for _ in range(0, length):
        s += random.choice(sel)
    return s

def is_close_msg(json_msg):
    return json_msg.get('__close__', False)

def is_cancel_msg(json_msg):
    return json_msg.get('__cancel__', False)

def mkdir_p(path):
    try:
        os.makedirs(path)
        os.chmod(path, stat.S_IROTH|stat.S_IWOTH|stat.S_IXOTH|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise

def validate_data_request(request):
    assert request["accept"]
    accept = request["accept"]

    if accept != "rosmsg":
        assert request["kafka_topic"]
        if "rosbag" in accept:
            output = accept.split("/", 1)[0]
            assert output in ["s3", "efs", "fsx"]

    assert request["vehicle_id"]
    assert request["scene_id"]
    assert request["sensor_id"]
    
    assert int(request["start_ts"]) > 0 
    assert int(request["stop_ts"]) > int(request["start_ts"]) 
    assert int(request["step"]) > 0

    if "ros" in accept:
        ros_topics = request["ros_topic"]
        data_types = request["data_type"]
        sensors = request["sensor_id"]
        for s in sensors:
            assert data_types[s]
            assert ros_topics[s]

def create_manifest(request=None, dbconfig=None, sensor_id=None):

    if sensor_id == 'bus':
        manifest = BusDataset(dbconfig=dbconfig, 
                        vehicle_id=request["vehicle_id"],
                        scene_id=request["scene_id"],
                        start_ts=int(request["start_ts"]), 
                        stop_ts=int(request["stop_ts"]),
                        step=int(request["step"]))
    else:
        manifest = ManifestDataset(dbconfig=dbconfig, 
                        vehicle_id=request["vehicle_id"],
                        scene_id=request["scene_id"],
                        sensor_id=sensor_id,
                        start_ts=int(request["start_ts"]), 
                        stop_ts=int(request["stop_ts"]),
                        step=int(request["step"]))

    return manifest

def load_json_from_file(path):
    with open(path, "r") as json_file:
        return json.load(json_file)

def delete_kafka_topics(bootstrap_servers=None, kafka_topics=None):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics(kafka_topics)
    admin.close()

def send_kafka_msg(bootstrap_servers=None, kafka_topic=None, kafka_msg=None):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=random_string())
    producer.send(kafka_topic, json.dumps(kafka_msg).encode('utf-8'))
    producer.flush()
    producer.close()