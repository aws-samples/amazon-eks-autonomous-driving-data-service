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

try:
    from queue import Empty # For Python 3.x
except ImportError:
    from Queue import  Empty  # For Python 2.7.x

import logging, time
import json
import time

import os
import shutil
import signal

from kafka import KafkaConsumer
from util import random_string, delete_kafka_topics, is_close_msg
from s3_reader import S3Reader
from s3_deleter import S3Deleter
from ros_util import RosUtil
from ros_util import ROS_VERSION

if ROS_VERSION == "1":
    import rosbag
    import rospy
elif ROS_VERSION == "2":
    import rclpy
    import rosbag2_py
    from rclpy.serialization import deserialize_message
    from rosidl_runtime_py.utilities import get_message
else:
    raise ValueError("Unsupported ROS_VERSION:" + str(ROS_VERSION))

class RosbagConsumer(Process):

    def __init__(self, servers=None, 
        response_topic=None, 
        s3=False, 
        use_time=None,
        no_playback=False,
        no_delete=False):
        Process.__init__(self)
        self.logger = logging.getLogger("rosbag_consumer")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self.servers = servers
        self.response_topic = response_topic
        self.tmp = os.getenv("TMP", default="/tmp")
        self.use_time = use_time
        
        self.no_playback = no_playback
        if self.no_playback:
            self.no_delete = True
            self.logger.info("Forcing 'no_delete' to True because 'no_playback' is True")
        else:
            self.no_delete = no_delete

        self.logger.info("Setting 'no_playback' to {}".format(no_playback))
        self.logger.info("Setting 'no_delete' to {}".format(no_delete))
        
        self.s3 = s3
        if self.s3 and not self.no_playback:
            self.s3_read_req = Queue()
            self.s3_read_resp = Queue()
            self.s3_delete_req = Queue()

        if not self.s3 and not self.no_delete:
            self.clean_up = set()

        if not self.no_playback:
            self.ros_publishers = dict()

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)

    
    def __get_ros_publishers(self, reader):
        topics_types = RosUtil.get_topics_types(reader)
        for ros_topic, data_type in topics_types.items():
            if  ros_topic not in self.ros_publishers:
                ros_data_class = RosUtil.get_data_class(data_type)
                if ROS_VERSION == "1":
                    self.ros_publishers[ros_topic] = rospy.Publisher(ros_topic, ros_data_class, queue_size=64)
                elif ROS_VERSION == "2":
                    self.ros_publishers[ros_topic] = self.ros2_node.create_publisher(ros_data_class, ros_topic, 64)
                time.sleep(1)
        

    def __publish_msgs(self, bag_path):

        if ROS_VERSION == "1":
            reader = rosbag.Bag(bag_path)
            self.__get_ros_publishers(reader)
            for ros_topic, ros_msg, _ in reader.read_messages():
                if self.use_time == "received":
                    RosUtil.set_ros_msg_received_time(ros_msg)
                self.ros_publishers[ros_topic].publish(ros_msg)
            reader.close()
        elif ROS_VERSION == "2":
            reader = rosbag2_py.SequentialReader()
            storage_options = rosbag2_py.StorageOptions(uri=bag_path, storage_id='mcap', storage_preset_profile='zstd_fast')
            converter_options = rosbag2_py.ConverterOptions(
                input_serialization_format='cdr',
                output_serialization_format='cdr')
            reader.open(storage_options, converter_options)
            self.__get_ros_publishers(reader)
            topics_types = RosUtil.get_topics_types(reader)
            while reader.has_next():
                ros_topic, ros_msg, _ = reader.read_next()
                msg_type = get_message(topics_types[ros_topic])
                ros_msg = deserialize_message(ros_msg, msg_type)
                if self.use_time == "received":
                    RosUtil.set_ros_msg_received_time(ros_msg)
                self.ros_publishers[ros_topic].publish(ros_msg)
            del reader

    def __read_s3(self, drain=False):
        bag_path = None
        try:
            try:
                msg = self.s3_read_resp.get(block=drain )
                bag_info = msg.split(" ")
                bag_path = bag_info[0]
                self.__publish_msgs(bag_path)

                if not self.no_delete:
                    self.s3_delete_req.put(msg, block=False)
            except Empty:
                pass
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.info(str(exc_type))
            self.logger.info(str(exc_value))

    def __publish_bag(self, json_msg):
        if self.s3:
            bag_bucket = json_msg["bag_bucket"]
            bag_prefix = json_msg["bag_prefix"]
            bag_name = json_msg["bag_name"]
            msg = bag_bucket + " " + bag_prefix + bag_name
            self.s3_read_req.put(msg)
            self.__read_s3()
        else:
            bag_path = json_msg['bag_path']
            self.__publish_msgs(bag_path)
            bag_dir = bag_path.rsplit('/', 1)[0]

            if not self.no_delete:
                self.clean_up.add(bag_dir)

    def run(self):
        
        try: 
            self.logger.info("starting rosbag_consumer:{0}".format(self.response_topic))
            node_name = "mozart_rosbag_{0}".format(random_string(6))
            if ROS_VERSION == "1":
                rospy.init_node(node_name)
            elif ROS_VERSION == "2":
                rclpy.init()
                self.ros2_node = rclpy.create_node(node_name)
                
            consumer = KafkaConsumer(self.response_topic, 
                                bootstrap_servers=self.servers,
                                auto_offset_reset="earliest",
                                client_id=random_string())

            if self.s3:
                if not self.no_playback:
                    self.s3_reader = S3Reader(self.s3_read_req, self.s3_read_resp)

                if not self.no_delete:
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

                    if not self.no_playback:
                        self.__publish_bag(json_msg)
                    
                    if self.no_playback or self.no_delete:
                        print(json_str)
                except Exception as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    print(str(exc_type))
                    print(str(exc_value))

            if self.s3:

                if not self.no_playback:
                    self.s3_read_req.put("__close__")
                    time.sleep(5)
                    self.__read_s3(drain=True)
                    
                    self.s3_reader.join(timeout=2)
                    if self.s3_reader.is_alive():
                        self.s3_reader.terminate()

                if not self.no_delete:
                    self.s3_delete_req.put("__close__")
                    time.sleep(5)
                    self.s3_deleter.join(timeout=2)
                    if self.s3_deleter.is_alive():
                        self.s3_deleter.terminate()
            else:
                if not self.no_delete:
                    for dir in self.clean_up:
                        shutil.rmtree(dir, ignore_errors=True)

            consumer.close()
            delete_kafka_topics(bootstrap_servers=self.servers, kafka_topics=[self.response_topic])

        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(exc_type))
            print(str(exc_value))
    
    def __exit_gracefully(self, signum, frame):
        self.logger.error("Received {} signal".format(signum))
        sys.exit(0)