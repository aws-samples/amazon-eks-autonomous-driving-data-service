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
import logging
from common.util import  validate_data_request, mkdir_p
import json
import time
import os
import shutil
from queue import Empty
from pathlib import Path
import signal

from kafka import KafkaConsumer, KafkaProducer
from common.util import  random_string, delete_kafka_topics, is_close_msg
from common.ros_util import RosUtil, ROS_VERSION

if ROS_VERSION == "1":
    import rosbag
    import rospy
    from common.s3_reader import S3Reader
elif ROS_VERSION == "2":
    import rclpy
    import rosbag2_py
    from rclpy.serialization import deserialize_message
    from rosidl_runtime_py.utilities import get_message
    from common.s3_directory_reader  import S3DirectoryReader
else:
    raise ValueError("Unsupported ROS_VERSION:" + str(ROS_VERSION))

class _RosbagConsumer:

    def __init__(self, consumer=None, s3_reader=None, use_time=None, no_playback=None, ros2_node=None):
    
        self.__logger = logging.getLogger("rosbag_consumer")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self.__ros2_node = ros2_node
        self.__consumer = consumer
        self.__use_time = use_time
        self.__no_playback = no_playback
        self.__logger.info(f"Setting 'no_playback' to {no_playback}")
        
        self.__s3_reader = s3_reader

        if not self.__no_playback:
            self.__ros_publishers = dict()

        self.__msg_count = 0

    def __get_ros_publishers(self, reader):
        topics_types = RosUtil.get_topics_types(reader)
        for ros_topic, data_type in topics_types.items():
            if  ros_topic not in self.__ros_publishers:
                ros_data_class = RosUtil.get_data_class(data_type)
                if ROS_VERSION == "1":
                    self.__ros_publishers[ros_topic] = rospy.Publisher(ros_topic, ros_data_class, queue_size=64)
                elif ROS_VERSION == "2":
                    self.__ros_publishers[ros_topic] = self.__ros2_node.create_publisher(ros_data_class, ros_topic, 64)
                time.sleep(1)
        

    def __publish_msgs(self, bag_path):

        if ROS_VERSION == "1":
            reader = rosbag.Bag(bag_path)
            self.__get_ros_publishers(reader)
            for ros_topic, ros_msg, _ in reader.read_messages():
                if self.__use_time == "received":
                    RosUtil.set_ros_msg_received_time(ros_msg)
                self.__ros_publishers[ros_topic].publish(ros_msg)
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
                if self.__use_time == "received":
                    RosUtil.set_ros_msg_received_time(ros_msg)
                self.__ros_publishers[ros_topic].publish(ros_msg)
            del reader

    def __read_s3(self, drain=False):
        try:
            while True:
                msg = self.__s3_reader.response_queue().get(block=drain )
                bag_info = msg.split(" ")
                bag_path = bag_info[0]
                assert os.path.exists(bag_path)
                self.__publish_msgs(bag_path)
                shutil.rmtree(bag_path, ignore_errors=True)
                self.__msg_count -= 1
                if not drain or self.__msg_count == 0:
                    break
        except Empty:
            pass
       
    def __publish_bag(self, json_msg):
        if self.__s3_reader:
            while True:
                try:
                    bag_bucket = json_msg["bag_bucket"]
                    bag_key = json_msg["bag_key"]
                    msg = bag_bucket + " " + bag_key
                    self.__s3_reader.request_queue().put(msg)
                    self.__msg_count += 1
                    self.__read_s3()
                    break
                except Exception:
                    exc_type, exc_value, _ = sys.exc_info()
                    self.__logger.warning(str(exc_type))
                    self.__logger.warning(str(exc_value))
                    time.sleep(2)
        else:
            bag_path = json_msg['bag_path']
            self.__publish_msgs(bag_path)

    def __call__(self):
        
        try: 
            start_ts = 0
            for msg in self.__consumer:
                try:
                    json_str = msg.value
                    json_msg = json.loads(json_str)

                    if json_msg['start_ts'] < start_ts:
                        self.__logger.warning(f"ignoring stale message {json_msg}; start_ts: {start_ts}")
                        continue

                    start_ts = json_msg['start_ts']

                    if is_close_msg(json_msg):
                        print(json_str)
                        break

                    if not self.__no_playback:
                        self.__publish_bag(json_msg)
                    
                    if self.__no_playback:
                        print(json_str)
                except Exception as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    print(str(exc_type))
                    print(str(exc_value))

            if self.__s3_reader:
                self.__read_s3(drain=True)
                self.__s3_reader.request_queue().put("__close__")
                    
                self.__s3_reader.join(timeout=2)
                if self.__s3_reader.is_alive():
                    self.__s3_reader.terminate()

        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(exc_type))
            print(str(exc_value))


class _ManifestConsumer:
    def __init__(self, consumer=None):
       
        self.__logger = logging.getLogger("manifest_consumer")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        
        self.__consumer = consumer
        
    def __call__(self):
        try:
            for message in self.__consumer:
                try:
                    json_str = message.value
                    json_msg = json.loads(json_str)
                    if is_close_msg(json_msg):
                        print(json_str)
                        break

                    print(json_str)
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))
                    break
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

class DataClient:
    def __init__(self, config):
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.__logger = logging.getLogger("data_client")
        self.__config = config
        self.__producer = KafkaProducer(bootstrap_servers=config["servers"], client_id=random_string())
        self.__response_topic = random_string()
        self.__consumer = KafkaConsumer(self.__response_topic, 
                                bootstrap_servers=self.__servers,
                                auto_offset_reset="earliest",
                                client_id=random_string())

        node_name = "mozart_rosbag_{0}".format(random_string(6))
        self.__cache_dir = os.path.join(Path.home(), ".cache", "s3", "rosbag", node_name)
        mkdir_p(node_name)
        
        self.__ros2_node = None

        if ROS_VERSION == "1":
            rospy.init_node(node_name)
            self.__s3_reader = S3Reader(cache_dir=self.__cache_dir)
        elif ROS_VERSION == "2":
            rclpy.init()
            self.__ros2_node = rclpy.create_node(node_name)
            self.__s3_reader = S3DirectoryReader(tmp_dir=self.__cache_dir)

        self.__s3_reader.start()

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)   

    @property
    def __use_time(self):
        return self.__config.get("use_time", "received")

    @property
    def __servers(self):
        return self.__config["servers"]
        
    def __exit_gracefully(self, signum, frame):
        self.__logger.info("Received {} signal".format(signum))
        self.close()
        sys.exit()

    def close(self):
        self.__producer.close()
        self.__consumer.close()
        delete_kafka_topics(bootstrap_servers=self.__servers, kafka_topics=[self.__response_topic])
        self.__close_s3_reader()
        shutil.rmtree(self.__cache_dir, ignore_errors=True)

    def __close_s3_reader(self):
        self.__s3_reader.request_queue().put("__close__")
        self.__s3_reader.join(timeout=2)
        if self.__s3_reader.is_alive():
            self.__s3_reader.terminate()

    def handle_request(self, request):
        self.__logger.info(f"validating data request {request}")
        validate_data_request(request)
        request["response_topic"] = self.__response_topic
        accept = request['accept']
        if accept.endswith("rosbag"):
            self.__request_rosbag(request=request)
        elif accept.endswith("manifest"):
            self.__request_manifest(request=request)
        else:
            self.__logger.error("Unexpected accept type: {0}".format(accept))
            raise ValueError()

    def __call__(self):
        try:
            delay = self.__config.get("delay")
            if delay:
                time.sleep(delay)

            requests = self.__config.get("requests")
            for request in requests:
                self.handle_request(request=request)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
    
    def __send_request_msg(self, request):
        msg = {"request": request}
        self.__logger.info(f"send request message: {msg}")
        self.__producer.send(request["kafka_topic"], json.dumps(msg).encode('utf-8'))
        self.__producer.flush()

    def __request_rosbag(self, request=None):
        try:
            s3 = request["accept"].startswith("s3/")
            no_playback=request.get("no_playback")
            if no_playback is not None:
                del request["no_playback"]
            else:
                no_playback = False

            s3_reader = self.__s3_reader if s3 and not no_playback else None
            rosbag_consumer = _RosbagConsumer(consumer=self.__consumer, s3_reader=s3_reader,
                no_playback=no_playback, use_time=self.__use_time, ros2_node=self.__ros2_node)
            self.__send_request_msg(request=request)
            rosbag_consumer()
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __request_manifest(self, request=None):
        try:
            manifest_consumer = _ManifestConsumer(consumer=self.__consumer)
            self.__send_request_msg(request=request)
            manifest_consumer()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data client')
    parser.add_argument('--config', type=str, help='configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)
        json_file.close()

        data_client = DataClient(config)
        data_client()
        data_client.close()
        sys.exit()
        