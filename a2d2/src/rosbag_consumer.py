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
from multiprocessing import Process,Queue
import threading, logging, time
import json
import random
import string
import time
import rosbag
import rospy
import os
import boto3
import shutil

from kafka import KafkaConsumer, KafkaAdminClient
from util import random_string, get_s3_client, get_data_class, get_topics_types, is_close_msg, mkdir_p
from s3_reader import S3Reader
from s3_deleter import S3Deleter

class RosbagConsumer(Process):
    def __init__(self, servers=None, response_topic=None, s3=False):
        Process.__init__(self)
        self.logger = logging.getLogger("rosbag_consumer")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self.servers = servers
        self.response_topic = response_topic
        self.tmp = os.getenv("TMP", default="/tmp")

        self.s3 = s3
        if self.s3:
            self.s3_read_req = Queue()
            self.s3_read_resp = Queue()
            self.s3_delete_req = Queue()

        if not self.s3:
            self.clean_up = set()

    def get_ros_publishers(self, bag_path):
        ros_publishers = dict()
        topics_types = get_topics_types(bag_path)
        for ros_topic, data_type in topics_types.items():
            ros_data_class = get_data_class(data_type)
            ros_publisher = rospy.Publisher(ros_topic, ros_data_class, queue_size=64)
            ros_publishers[ros_topic] = ros_publisher
        return ros_publishers

    def read_s3(self, drain=False):
        bag_path = None
        try:
            msg = self.s3_read_resp.get(block=drain)
            bag_info = msg.split(" ")
            bag_path = bag_info[0]
            bucket = bag_info[1]
            key = bag_info[2]

            ros_publishers = self.get_ros_publishers(bag_path)

            bag = rosbag.Bag(bag_path)
            for ros_topic, ros_msg, _ in bag.read_messages():
                ros_publishers[ros_topic].publish(ros_msg)
            bag.close()
            self.s3_delete_req.put(msg)
        except:
            pass

    def publish_bag(self, json_msg):
        if self.s3:
            bag_bucket = json_msg["bag_bucket"]
            bag_prefix = json_msg["bag_prefix"]
            bag_name = json_msg["bag_name"]
            msg = bag_bucket + " " + bag_prefix + bag_name
            self.s3_read_req.put(msg)
            self.read_s3()
        else:
            bag_path = json_msg['bag_path']
            ros_publishers = self.get_ros_publishers(bag_path)

            bag = rosbag.Bag(bag_path)
            for ros_topic, ros_msg, _ in bag.read_messages():
                ros_publishers[ros_topic].publish(ros_msg)
            bag.close()

            bag_dir = bag_path.rsplit('/', 1)[0]
            self.clean_up.add(bag_dir)

    def run(self):
        
        try: 
            self.logger.info("starting rosbag_consumer:{0}".format(self.response_topic))
            rospy.init_node("mozart_rosbag_{0}".format(random_string(6)))

            consumer = KafkaConsumer(self.response_topic, 
                                bootstrap_servers=self.servers,
                                client_id=random_string())

            if self.s3:
                self.s3_reader = S3Reader(self.s3_read_req, self.s3_read_resp)
                self.s3_deleter = S3Deleter(self.s3_delete_req)
                self.s3_reader.start()
                self.s3_deleter.start()

            for msg in consumer:
                try:
                    json_str = msg.value
                    json_msg = json.loads(json_str)

                    if is_close_msg(json_msg):
                        print(json_str)
                        break

                    self.publish_bag(json_msg)
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    print(str(e))
                    break

            if self.s3:
                self.read_s3(drain=True)
                self.s3_read_req.put("__close__")
                self.s3_reader.join(timeout=2)
                if self.s3_reader.is_alive():
                    self.s3_reader.terminate()
                self.s3_delete_req.put("__close__")
                time.sleep(5)
                self.s3_deleter.join(timeout=2)
                if self.s3_deleter.is_alive():
                    self.s3_deleter.terminate()
            else:
                for dir in self.clean_up:
                    shutil.rmtree(dir, ignore_errors=True)

            consumer.close()
            admin = KafkaAdminClient(bootstrap_servers=self.servers)
            admin.delete_topics([self.response_topic])
            admin.close()

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))
