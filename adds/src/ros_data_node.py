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

from abc import ABC, abstractmethod

import sys, traceback
import json
import os, time
import threading
import glob
from hashlib import sha224
from typing import Any, Callable, Union
import boto3
import signal
import shutil
import logging
from common.bus_dataset import BusDataset
from common.manifest_dataset import ManifestDataset

from common.util import  get_s3_client, get_data_requests
from common.util import  create_manifest, s3_bucket_keys, mkdir_p
from common.thread_utils import join_thread_timeout_retry
from common.s3_reader import S3Reader
from common.s3_writer import S3Writer
from common.ros_util import RosUtil, ROS_VERSION

if ROS_VERSION == "1":
    import rospy
    import rosbag
elif ROS_VERSION == "2":
    import rosbag2_py
    from rclpy.serialization import serialize_message
else:
    raise ValueError("Unsupported ROS_VERSION:" + str(ROS_VERSION))

class RosDataNode(ABC):
    PLAY = "play"
    PAUSE = "pause"
    STOP = "stop"
    MAX_RATE = "max_rate"
    LOG_HEARBEAT_INTERVAL_SEC = 300
    LOAD_DATA_TIMEOUT = 15
    MAX_LOAD_DATA_RETRY = 4

    def __init__(self, config: dict):
        self.__logger = logging.getLogger("ros_data_node")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self._config = config
        calibration=config['calibration']
        cal_bucket = calibration["cal_bucket"]
        cal_key = calibration["cal_key"]

        self._s3_cache_dir = f"/efs/.cache/s3/{cal_bucket}"
        mkdir_p(self._s3_cache_dir)
        self._s3_client = get_s3_client()
        dataset = config["dataset"]
        self.__schema = dataset["schema_name"]
        self.__logger.info(f"Using schema: {self.__schema}")

        rosutil_classname = dataset["rosutil_classname"]
        self.__logger.info(f"Importing RosUtil class: {rosutil_classname}")
        RosUtilClass = RosUtil.dynamic_import(rosutil_classname)
        self.__logger.info(f"Creating RosUtil object for {rosutil_classname}")
        self.__ros_util = RosUtilClass(bucket=cal_bucket, key=cal_key)

        dynamodb = config['dynamodb']
        dynamodb_resource = boto3.resource('dynamodb')
        self.__data_request_table = dynamodb_resource.Table(dynamodb['data_request_table'])

        RosUtil.create_cv_brigde()

        self.__s3_readers = dict()
        self.__start_s3_writer()

        self._accept = None
        self.__bag = None
        self.__bag_path = None
        self.__bag_name = None
        self.__bag_dir = None
        
        self.__logger.info(f"Initialization complete {config}") 

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)   

     
    @abstractmethod
    def spin(self):
        pass

    @abstractmethod
    def _send_response_msg(self, json_msg: dict):
        pass

    @abstractmethod
    def _ros2_node(self):
        pass

    @property
    def _s3_config(self) -> dict:
        return self._data_store["s3"]

    @property
    def _fsx_config(self) -> dict:
        return self._data_store["fsx"]

    @property
    def _efs_config(self) -> dict:
        return self._data_store["efs"]

    @property
    def _rosbag_bucket(self) -> str:
        return self._s3_config["rosbag_bucket"]

    @property
    def _rosbag_prefix(self) -> str:
        return self._s3_config["rosbag_prefix"]

    @property
    def _data_store(self) -> dict:
        return self._config["data_store"]

    @property
    def _dbconfig(self) -> dict:
        return self._config["database"]

    def _exit_gracefully(self, signum, frame):
        self.__logger.info("Received {} signal".format(signum))
        self.__close_s3_writer()
        if self.__bag_dir:
            shutil.rmtree(self.__bag_dir, ignore_errors=True)
        self.__close_s3_readers()
        sys.exit()

    def _set_request_state(self, state: str):
        self.__request_state = state

    def _set_max_rate(self, max_rate: float):
        if max_rate > 0:
            self.__sleep_interval = (len(self.__sensor_list)/max_rate)
        else:
            self.__sleep_interval  = 0

        self.__logger.info("interleaving sleep_interval:" + str(self.__sleep_interval))

    def _init_request(self, request: dict):

        self.__logger.info(f"Initializing request: {request}")
        self.__request_index = str(int(time.time()*10**6))

        self.__manifests = dict() 
        self.__sensor_dict = dict()
        self.__sensor_list = list()
        self.__sensor_active = dict()
        self.__sensor_index = 0
        self.__round_robin = list()
        self.__sensor_transform = dict()
        self.__sensor_data_type = dict()

        self._request = request

        self._accept = self._request['accept']
        sensors = self._request['sensor_id']
        lidar_view = self._request.get("lidar_view", "camera")
        image_request = self._request.get("image", "original")
       
        for sensor in sensors:
            data_type = self._request["data_type"][sensor]
            self.__sensor_data_type[sensor] = data_type
            self.__manifests[sensor] = create_manifest(request=request, dbconfig=self._dbconfig, sensor_id=sensor, schema=self.__schema)
            self.__sensor_dict[sensor] = []
            self.__sensor_list.append(sensor)
            self.__sensor_active[sensor] = True

            if lidar_view == "vehicle" and ( data_type == RosUtil.PCL_DATA_TYPE or data_type == RosUtil.MARKER_ARRAY_CUBE_DATA_TYPE) :
                self.__sensor_transform[sensor] = self.__ros_util.sensor_to_vehicle(sensor=sensor)
            elif  image_request == "undistorted" and data_type == RosUtil.IMAGE_DATA_TYPE:
                self.__sensor_transform[sensor] = self.__ros_util.get_undistort_fn(sensor=sensor)

        self._set_max_rate(self._request.get(self.MAX_RATE, 0))

        self.__ros_publishers = dict()
        self.__latest_msg_ts = 0 if len(self.__sensor_list) > 1 else float('inf')
        self._sync_bus = self._request.get("sync_bus", True)
        
        if self._accept.endswith("rosbag"):
            if len(sensors) > 1:
                self.__bag_lock = threading.Lock()
            else:
                self.__bag_lock = None

            self.__storage_id = self._request.get('storage_id', 'mcap')
            self.__storage_preset_profile = self._request.get("storage_preset_profile", "zstd_fast")
        
            self.__multipart = 'multipart' in self._accept

            if self.__multipart:
                self.msg_count = 0

                if ROS_VERSION == "1":
                    self.chunk_count = len(self.__sensor_list)*self._request.get("multipart_nmsgs", 2)
                elif ROS_VERSION == "2":
                    self.chunk_count = len(self.__sensor_list)*self._request.get("multipart_nmsgs", 3)
             
            self.__create_bag_dir()

        self._set_request_state(self.PLAY)

        self.__logger.info(f"Initialized request: {request}")

    def _handle_manifest_request(self):

        try:
            tasks = []
            sensors = self._request["sensor_id"]
            
            for sensor in sensors:
                manifest = self.__manifests[sensor] 
                t = threading.Thread(target=self.__publish_manifest, name=sensor, kwargs={"manifest": manifest})
                tasks.append(t)
                t.start()
                self.__logger.info("Started thread:" + t.name)
            
            for t in tasks:
                self.__logger.info("Wait on thread:" + t.name)
                t.join()
                self.__logger.info("Thread finished:" + t.name)

            json_msg = {"request": self._request, "__close__": True}
            self._send_response_msg(json_msg)
          
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def _handle_rosbag_request(self):

        try:
            if self.__cached_location:
                self.__handle_cached_bag_request()
            else:
                tasks = []

                sensors = self._request["sensor_id"]
                sensor_frame_id = self._request.get("frame_id", dict())

                for sensor in sensors:
                    manifest = self.__manifests[sensor]
                    frame_id = sensor_frame_id.get(sensor, "map")

                    t = threading.Thread(target=self.__record_sensor, name=sensor,
                        kwargs={"manifest": manifest,  "sensor":  sensor, "frame_id": frame_id})
                    tasks.append(t)
                    t.start()
                    self.__logger.info("Started thread:" + t.name)
                
                for t in tasks:
                    self.__logger.info("Wait on thread:" + t.name)
                    t.join()
                    self.__logger.info("Thread finished:" + t.name)

                self.__logger.info("Flush ROS sensors")
                self.__flush_sensors()

                self.__logger.info("Close ROS bag")
                self.__close_bag()

            self.__logger.info(f"Request completed: {self._request}")
            self.__bag_request_completed()
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def _handle_rosmsg_request(self):

        try:
            tasks = []

            sensors = self._request["sensor_id"]
            sensor_topics = self._request['ros_topic']
            sensor_data_types = self._request["data_type"]
            sensor_frame_id = self._request.get("frame_id", dict())

            for sensor in sensors:
                manifest = self.__manifests[sensor]
                data_type = sensor_data_types[sensor]
                ros_topic = sensor_topics[sensor]
                frame_id = sensor_frame_id.get(sensor, "map")

                ros_data_class = self.__ros_util.get_data_class(data_type)
                if ROS_VERSION == "1":
                    self.__ros_publishers[sensor] = rospy.Publisher(ros_topic, ros_data_class, queue_size=64)
                elif ROS_VERSION == "2":
                    self.__ros_publishers[sensor] = self._ros2_node().create_publisher(ros_data_class, ros_topic, 64)
                time.sleep(1)
                t = threading.Thread(target=self.__publish_sensor, name=sensor,
                    kwargs={"manifest": manifest,  "sensor":  sensor, "frame_id": frame_id})
                tasks.append(t)
                t.start()
                self.__logger.info("Started thread:" + t.name)
            
            for t in tasks:
                self.__logger.info("Wait on thread:" + t.name)
                t.join()
                self.__logger.info("Thread finished:" + t.name)

            self.__logger.info("Flush ROS sensors")
            self.__flush_sensors()

            json_msg = {
                "request": self._request,
                "__close__": True
            }
            self._send_response_msg(json_msg)
          
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))


    def __handle_cached_bag_request(self):

        self.__logger.info("Handle cached bag request")

        try:
            if self._accept.startswith("s3/"):
                bucket_prefix = self.__cached_location
                keys = []
                for key in s3_bucket_keys(self._s3_client, self._rosbag_bucket, bucket_prefix):
                    if ".bag" in key:
                        parts = key.split(".bag")
                        keys.append(f"{parts[0]}.bag")
                
                keys = list(set(keys))
                keys.sort()
                for key in keys:
                    json_msg = { 
                                "request": self._request,
                                "output": "s3", 
                                "bag_bucket": self._rosbag_bucket, 
                                "bag_key": key,
                                "multipart": self.__multipart
                            } 
                    self._send_response_msg(json_msg)
            elif self._accept.startswith("fsx/"):
                files = glob.glob(f"{self.__cached_location}/*.bag")
                files.sort()
                for bag_path in files:
                    json_msg = { 
                            "request": self._request,
                            "output": "fsx", 
                            "bag_path": bag_path , 
                            "multipart": self.__multipart
                        } 
                    self._send_response_msg(json_msg)
            elif self._accept.startswith("efs/"):
                files = glob.glob(f"{self.__cached_location}/*.bag")
                files.sort()
                for bag_path in files:
                    json_msg = { 
                            "request": self._request,
                            "output": "efs", 
                            "bag_path": bag_path, 
                            "multipart": self.__multipart
                        } 
                    self._send_response_msg(json_msg)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

            self.__data_request_table.delete_item(Key={'request_hash': self.__request_hash, 'request_index': self.__cached_index})
    
    def __is_sensor_alive(self, sensor):
        return  self.__sensor_dict[sensor] or self.__sensor_active[sensor]
    
    def __round_robin_sensor(self,  sensor=None, msg=None):
        # add message to sensor queue
        self.__sensor_dict[sensor].append(msg)
                
        msg = None
        sensor = None

        # round robin through sensors
        _nsensors = len(self.__sensor_list)
        for _ in self.__sensor_list:
            self.__sensor_index = (self.__sensor_index + 1) % _nsensors
            _sensor = self.__sensor_list[ self.__sensor_index ]
            if _sensor in self.__round_robin and any([True for k in self.__sensor_active.keys() if k not in self.__round_robin and self.__is_sensor_alive(k)]):
                continue

            sensor_msg_list = self.__sensor_dict[_sensor]
            if sensor_msg_list:
                if self.__is_bus_sensor(_sensor) and self._sync_bus:
                    msg_list = RosUtil.drain_ros_msgs( ros_msg_list=sensor_msg_list,  drain_ts=self.__latest_msg_ts)
                    if msg_list:
                        msg = msg_list[-1]
                    else:
                       continue
                else:
                    msg = sensor_msg_list.pop(0)
                sensor = _sensor
                break

        return sensor, msg

    def __flush_sensors(self):
        try:
            _nsensors = len(self.__sensor_list) 
            flushed = []
            # rotate through sensors and flush them
            self.__logger.info("Flushing  {} sensors".format(_nsensors))
            while len(flushed) < _nsensors:
                msg = None
                sensor = None

                self.__sensor_index = (self.__sensor_index + 1) % _nsensors
                _sensor = self.__sensor_list[ self.__sensor_index ]
                
                sensor_msg_list = self.__sensor_dict[_sensor]
                if sensor_msg_list:
                    if self.__is_bus_sensor(_sensor) and self._sync_bus:
                        msg_list = RosUtil.drain_ros_msgs( ros_msg_list=sensor_msg_list,  drain_ts=self.__latest_msg_ts)
                        if msg_list:
                            msg = msg_list[-1]
                            
                    if not msg:
                        msg = sensor_msg_list.pop(0)
                    sensor = _sensor
                else:
                    if _sensor not in flushed:
                        flushed.append(_sensor)

                if sensor and msg:
                    if self._accept.endswith("rosbag"):
                        self.__write_ros_msg_to_bag(sensor=sensor, msg=msg)
                    elif self._accept.endswith("rosmsg"):
                        self.__publish_ros_msg(ros_msg=msg, sensor=sensor)
            
                if self.__sleep_interval > 0:
                    time.sleep(self.__sleep_interval)
            
            self.__logger.info("Flushed {} sensors".format(len(flushed)))

        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
            raise

    def __is_bus_sensor(self, sensor=None):
        return self._request["data_type"][sensor] == self.__ros_util.get_bus_datatype()

    def __get_s3_reader(self, sensor):
        s3_reader = self.__s3_readers.get(sensor)
        if s3_reader is None:
            s3_reader = S3Reader(name=sensor, cache_dir=self._s3_cache_dir)
            self.__s3_readers[sensor] = s3_reader
            self.__logger.info(f"Start s3_reader for {sensor}")
            s3_reader.start()
            self.__logger.info(f"PID s3_reader {sensor}: {s3_reader.pid}")
            

        return s3_reader

    def __get_cached_request(self):
       
        self.__cached_location = None
        self.__cached_index = None

        request_encoding = bytes(json.dumps(self._request), "utf-8")
        self.__request_hash = sha224(request_encoding).hexdigest()

        for cached_requests in get_data_requests(data_request_table=self.__data_request_table, request_hash=self.__request_hash, ros_version=ROS_VERSION):
            for cached_request in cached_requests:

                request = cached_request.get('request_json')
                location = cached_request.get('request_location')
                index = cached_request.get('request_index')
                if location and request == self._request:
                    valid_cache = False
                    
                    if self._accept.startswith('s3/'):
                        for key in s3_bucket_keys(self._s3_client, self._rosbag_bucket, location):
                            if ".bag" in key:
                                valid_cache = True
                                break
                    else:
                        files = glob.glob(f"{location}/*.bag")
                        valid_cache =  files and len(files) > 0

                    if valid_cache:
                        self.__logger.info(f"cached request is valid: {request}")
                        self.__cached_location = location
                        self.__cached_index = index
                    else:
                        self.__logger.warning(f"cached request is not valid: {request}")
                        self.__data_request_table.delete_item(Key={'request_hash': self.__request_hash, 'request_index': index})
                    return

    def __guard(self):
        while self.__request_state == self.PAUSE:
            time.sleep(1)

        return self.__request_state == self.PLAY

    def __start_s3_writer(self):
        self.__logger.info("Starting S3 writer")
        self.__s3_writer = S3Writer(delete_local_path=True)
        self.__s3_writer.start()
        self.__logger.info("S3 writer started")

    def __close_s3_writer(self):
        self.__logger.info("Closing S3 writer")
        if self.__s3_writer:
            self.__s3_writer.request_queue().put("__close__")
            self.__s3_writer.join(timeout=2)
            if self.__s3_writer.is_alive():
                self.__s3_writer.terminate()
        self.__logger.info("S3 writer closed")
        
    def __close_s3_readers(self):
        for s3_reader in self.__s3_readers.values():
            s3_reader.request_queue().put("__close__")
            s3_reader.join(timeout=2)
            if s3_reader.is_alive():
                s3_reader.terminate()

    def __create_bag_dir(self):

        self.__get_cached_request()
        if self.__cached_location is None:
            self.__logger.info("Creating rosbag directory")
            if self._accept.startswith('s3/'):
                self.__bag_dir = os.path.join(self._s3_cache_dir, 
                        self._rosbag_prefix, 
                        self.__request_hash, 
                        self.__request_index )
            elif self._accept.startswith('fsx/'):
                self.__bag_dir = os.path.join(self._fsx_config['rosbag'], self.__request_hash, self.__request_index)
            elif self._accept.startswith('efs/'):
                self.__bag_dir = os.path.join(self._efs_config['rosbag'], self.__request_hash, self.__request_index)
            else:
                raise ValueError(f"Invalid {self._accept}")
                
            mkdir_p(self.__bag_dir)
            self.__logger.info(f"Rosbag directory: {self.__bag_dir}")

    def __bag_request_completed(self):
        if self._accept.startswith("s3/"):
            location = f"{self._rosbag_prefix}{self.__request_hash}/{self.__request_index}/" 
        else:
            location = self.__bag_dir

        json_msg = {"request": self._request, "rosbag_location": location, "__close__": True}
        self._send_response_msg(json_msg)

        if self.__cached_location is None:
            self.__logger.info(f"Cache request: {self._request}")
            response = self.__data_request_table.put_item(
                Item={
                    'request_hash': self.__request_hash,
                    'request_index': self.__request_index,
                    'request_json': self._request,
                    'request_location': location,
                    'ros_version': ROS_VERSION
                }
            )
            self.__logger.info(f"Caching response: {response}")
        
    def __open_bag(self):

        ts = str(int(time.time()*10**6))
        self.__bag_name = f"{ts}.bag"
        self.__bag_path = os.path.join(self.__bag_dir, self.__bag_name)
        if ROS_VERSION == "1":
            self.__bag = rosbag.Bag(self.__bag_path, 'w')
        elif ROS_VERSION == "2":
            self.__bag = rosbag2_py.SequentialWriter()
            storage_options = rosbag2_py.StorageOptions(uri=self.__bag_path, storage_id=self.__storage_id, 
                storage_preset_profile=self.__storage_preset_profile)
            converter_options = rosbag2_py.ConverterOptions(
                input_serialization_format='cdr',
                output_serialization_format='cdr')
            self.__bag.open(storage_options, converter_options)

            sensors = self._request["sensor_id"]
            for s in sensors:
                data_type = self._request["data_type"][s]
                ros_topic = self._request['ros_topic'][s]
                topic_info = rosbag2_py.TopicMetadata(
                        name=ros_topic,
                        type=data_type,
                        serialization_format='cdr')
                self.__bag.create_topic(topic_info)

    def __close_bag(self):
        if self.__bag:
            if ROS_VERSION == "1":
                self.__bag.close()
            elif ROS_VERSION == "2":
                del self.__bag

            if self._accept.startswith("s3/"):
                bag_key = f"{self._rosbag_prefix}{self.__request_hash}/{self.__request_index}/{self.__bag_name}"
                json_msg = { 
                        "request": self._request,
                        "output": "s3", 
                        "bag_bucket": self._rosbag_bucket, 
                        "bag_key": bag_key, 
                        "multipart": self.__multipart
                    } 
                
                self.__s3_writer.request_queue().put(self.__bag_path + " " + self._rosbag_bucket + " " + bag_key)
                self.__s3_writer.response_queue().get(block=True)
            elif self._accept.startswith("fsx/"):
                json_msg = { 
                            "request": self._request,
                            "output": "fsx", 
                            "bag_path": self.__bag_path , 
                            "multipart": self.__multipart
                        } 
            elif self._accept.startswith("efs/"):
                json_msg = { 
                        "request": self._request,
                        "output": "efs", 
                        "bag_path": self.__bag_path , 
                        "multipart": self.__multipart
                    } 

            self._send_response_msg(json_msg)

            self.__bag = None
            self.__bag_path = None
            self.__bag_name = None

    def __write_ros_msg_to_bag(self, sensor: str, msg: Any):

        if not self.__bag:
            self.__open_bag()
        topic = self._request['ros_topic'][sensor]
        if ROS_VERSION == "1":
            self.__bag.write(topic, msg)
        elif ROS_VERSION == "2":
            self.__bag.write(topic, serialize_message(msg), int(time.time()*1000000))

        if not self.__is_bus_sensor(sensor):
            msg_ts = RosUtil.get_ros_msg_ts_nsecs(msg)
            if msg_ts > self.__latest_msg_ts or self.__latest_msg_ts == float('inf'):
                self.__latest_msg_ts = msg_ts

            self.__round_robin.append(sensor)

        if self.__multipart:
            self.msg_count += 1
            if self.msg_count % self.chunk_count == 0:
                self.__close_bag()

    def __record_sensor_data(self, sensor: str, ts: float, frame_id: str, ros_msg_fn: Callable, params: dict):
        try:
            ros_msg = ros_msg_fn(**params)
            RosUtil.set_ros_msg_header( ros_msg=ros_msg, ts=ts, frame_id=frame_id)

            if self.__bag_lock:
                self.__bag_lock.acquire()

            sensor, ros_msg = self.__round_robin_sensor(sensor=sensor, msg=ros_msg)
            if sensor and ros_msg:
                self.__write_ros_msg_to_bag(sensor=sensor, msg=ros_msg)

            sensors = [k for k in self.__sensor_active.keys() if not self.__is_bus_sensor(k) and self.__is_sensor_alive(k)]
            if (len(self.__round_robin) >= len(sensors)) and self.__round_robin:
                self.__round_robin.pop(0)
                
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
            raise
        finally:
            if self.__bag_lock:
                self.__bag_lock.release()

    def __record_sensor_from_fs(self, manifest: ManifestDataset,  sensor: str, frame_id: str):
        data_loader = dict()
        data = dict() 
        ts = dict()

        data_type = self.__sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        transform =   self.__sensor_transform.get(sensor, None)

        prev = time.time()
        while True:
            now = time.time()
            if now - prev > self.LOG_HEARBEAT_INTERVAL_SEC:
                self.__logger.info(f"Thread {sensor} is alive")
                prev = now

            files = None
            while not files and manifest.is_open():
                files = manifest.fetch()
            if not files:
                break

            count = RosUtil.load_data_from_fs(data_type=data_type,
                data_store=self._data_store, data_files=files, data_loader=data_loader, data=data, ts=ts)
        
            for i in range(0, count):
                join_thread_timeout_retry(name=files[i], 
                                          t=data_loader[i], 
                                          timeout=self.LOAD_DATA_TIMEOUT, 
                                          max_retry=self.MAX_LOAD_DATA_RETRY, 
                                          logger=self.__logger)
                try:
                    params = RosUtil.get_ros_msg_fn_params(data_type=data_type, data=data[i], 
                        sensor=sensor, request=self._request, transform=transform)
                    self.__record_sensor_data(sensor=sensor, ts=ts[i], frame_id=frame_id, ros_msg_fn=ros_msg_fn, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break

        self.__sensor_active[sensor] = False

    def __record_sensor_from_s3(self, manifest: ManifestDataset, sensor: str, frame_id: str):

        s3_reader = self.__get_s3_reader(sensor)
        req = s3_reader.request_queue()
        resp = s3_reader.response_queue()

        data_type = self.__sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        data_load_fn = self.__ros_util.get_data_load_fn(data_type=data_type)
        transform = self.__sensor_transform.get(sensor, None)

        prev = time.time()

        while True:
            now = time.time()
            if now - prev > self.LOG_HEARBEAT_INTERVAL_SEC:
                self.__logger.info(f"Thread {sensor} is alive")
                prev = now

            files = None
            while not files and manifest.is_open():
                files = manifest.fetch()
            if not files:
                break

            for f in files:
                bucket = f[0]
                key = f[1]
                req.put(bucket+" "+key)

            for f in files:
                try:
                    path = resp.get(block=True).split(" ", 1)[0]
                    data = data_load_fn(path)
                    ts = int(f[2])
                    params = RosUtil.get_ros_msg_fn_params(data_type=data_type, 
                        data=data, sensor=sensor, request=self._request, transform=transform)
                    self.__record_sensor_data(sensor=sensor, ts=ts, frame_id=frame_id, 
                        ros_msg_fn=ros_msg_fn, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break

        self.__sensor_active[sensor] = False

    def __record_bus(self, manifest: BusDataset, sensor: str, frame_id: str):
        ros_msg_fn=self.__ros_util.bus_msg
        while True:
            rows = None
            while not rows and manifest.is_open():
                rows = manifest.fetch()
            if not rows:
                break
        
            for row in rows:
                try:
                    params = {"row": row}
                    ts=row[2]
                    self.__record_sensor_data(sensor=sensor, ts=ts, frame_id=frame_id, ros_msg_fn=ros_msg_fn, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break
        
        self.__sensor_active[sensor] = False

    def __record_sensor(self, manifest: Union[BusDataset, ManifestDataset], sensor: str, frame_id: str):
        try:
            data_type = self._request["data_type"][sensor]
            if data_type ==  self.__ros_util.get_bus_datatype():
                self.__record_bus(manifest=manifest, sensor=sensor, frame_id=frame_id)
            else:
                if self._data_store['input'] != 's3':
                    self.__record_sensor_from_fs(manifest=manifest, sensor=sensor, frame_id=frame_id)
                else:
                    self.__record_sensor_from_s3(manifest=manifest, sensor=sensor, frame_id=frame_id)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))


    def __publish_ros_msg(self, ros_msg: Any, sensor: str):

        self.__ros_publishers[sensor].publish(ros_msg)
        if not self.__is_bus_sensor(sensor):
            msg_ts = RosUtil.get_ros_msg_ts_nsecs(ros_msg)
            if msg_ts > self.__latest_msg_ts or self.__latest_msg_ts == float('inf'):
                self.__latest_msg_ts = msg_ts

            self.__round_robin.append(sensor)

    def __publish_sensor_data(self, sensor: str, 
                        ts: float, 
                        frame_id: str, 
                        ros_msg_fn: Callable, 
                        params: dict):
        try:
            ros_msg = ros_msg_fn(**params)
            RosUtil.set_ros_msg_header( ros_msg=ros_msg, ts=ts, frame_id=frame_id)

            sensor, msg = self.__round_robin_sensor(sensor=sensor, msg=ros_msg)
            if sensor and msg:
                self.__publish_ros_msg(ros_msg=msg, sensor=sensor)

            sensors = [k for k in self.__sensor_active.keys() if not self.__is_bus_sensor(k) and self.__is_sensor_alive(k)]
            if (len(self.__round_robin) >= len(sensors)) and self.__round_robin:
                self.__round_robin.pop(0)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
            raise

    def __publish_sensor_from_fs(self, manifest: ManifestDataset, sensor: str, frame_id: str):

        data_loader = dict()
        data = dict() 
        ts = dict()
 
        data_type = self.__sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        transform =   self.__sensor_transform.get(sensor, None)

        prev = time.time()
        while self.__guard():
            now = time.time()
            if now - prev > self.LOG_HEARBEAT_INTERVAL_SEC:
                self.__logger.info(f"Thread {sensor} is alive")
                prev = now

            files = None
            while not files and manifest.is_open():
                files = manifest.fetch()
            if not files:
                break

            count = RosUtil.load_data_from_fs(data_type=data_type, data_store=self._data_store, 
                data_files=files, data_loader=data_loader, data=data, ts=ts)
        
            for i in range(0, count):
                join_thread_timeout_retry(name=files[i], 
                                          t=data_loader[i], 
                                          timeout=self.LOAD_DATA_TIMEOUT, 
                                          max_retry=self.MAX_LOAD_DATA_RETRY, 
                                          logger=self.__logger)
                try:
                    params = RosUtil.get_ros_msg_fn_params(data_type=data_type, data=data[i], 
                        sensor=sensor, request=self._request, transform=transform)
                    self.__publish_sensor_data(sensor=sensor, ts=ts[i], frame_id=frame_id, ros_msg_fn=ros_msg_fn, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break

            if self.__sleep_interval > 0:
                time.sleep(self.__sleep_interval)

        self.__sensor_active[sensor] = False

    def __publish_sensor_from_s3(self, manifest: ManifestDataset, sensor: str, frame_id: str):

        s3_reader = self.__get_s3_reader(sensor)
        req = s3_reader.request_queue()
        resp = s3_reader.response_queue()

        data_type = self.__sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        data_load_fn = RosUtil.get_data_load_fn(data_type=data_type)
        transform =   self.__sensor_transform.get(sensor, None)

        prev = time.time()
        while self.__guard():
            now = time.time()
            if now - prev > self.LOG_HEARBEAT_INTERVAL_SEC:
                self.__logger.info(f"Thread {sensor} is alive")
                prev = now
           
            files = None
            while not files and manifest.is_open():
                files = manifest.fetch()
            if not files:
                break

            for f in files:
                bucket = f[0]
                key = f[1]
                req.put(bucket+" "+key)

            for f in files:
                try:
                    path = resp.get(block=True).split(" ", 1)[0]
                    data = data_load_fn(path)
                    ts = int(f[2])
                    params = RosUtil.get_ros_msg_fn_params(data_type=data_type, 
                        data=data, sensor=sensor, request=self._request, transform=transform)
                    self.__publish_sensor_data(sensor=sensor, ts=ts, frame_id=frame_id, ros_msg_fn=ros_msg_fn, params=params)

                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break

            if self.__sleep_interval > 0:
                time.sleep(self.__sleep_interval)
        
        self.__sensor_active[sensor] = False

    def __publish_bus(self, manifest: BusDataset, sensor: str,  frame_id: str):

        while self.__guard():
            rows = None
            while not rows and manifest.is_open():
                rows = manifest.fetch()
            if not rows:
                break
        
            for row in rows:
                try:
                    params = {"row": row}
                    self.__publish_sensor_data(sensor=sensor, ts=row[2], frame_id=frame_id, 
                        ros_msg_fn=self.__ros_util.bus_msg, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.__logger.error(str(exc_type))
                    self.__logger.error(str(exc_value))

            if self._request.get('preview', False):
                break
        
        self.__sensor_active[sensor] = False

    def __publish_sensor(self, manifest: Union[BusDataset, ManifestDataset], sensor: str, frame_id: str):
        try:
            data_type = self._request["data_type"][sensor]
            if data_type ==  self.__ros_util.get_bus_datatype():
                self.__publish_bus(manifest=manifest, sensor=sensor, frame_id=frame_id)
            else:
                if self._data_store['input'] != 's3':
                    self.__publish_sensor_from_fs(manifest=manifest, sensor=sensor, frame_id=frame_id)
                else:
                    self.__publish_sensor_from_s3(manifest=manifest, sensor=sensor, frame_id=frame_id)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __publish_manifest(self, manifest: Union[BusDataset, ManifestDataset]):
        try:
            while True:
                content = manifest.fetch()
                if not content:
                    break

                json_msg = { "request": self._request, "manifest": content}  
                self._send_response_msg(json_msg=json_msg)

                if self._request.get('preview', False):
                    break
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))