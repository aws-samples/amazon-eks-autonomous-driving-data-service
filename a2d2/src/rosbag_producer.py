
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
from multiprocessing import Process, Queue
import logging
import json
import os
import threading
import time
import signal

import rosbag

from kafka import KafkaProducer
from util import random_string, get_s3_resource
from util import get_s3_client, mkdir_p, create_manifest
from s3_reader import S3Reader
from ros_util import RosUtil

class Qmsg:
    def __init__(self, msg=None, ts=None):
        self.msg = msg
        self.ts = ts

class RosbagProducer(Process):
    
    def __init__(self, dbconfig=None, servers=None,
                request=None, data_store=None, calibration=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("rosbag_producer")

        self.servers = servers
        self.request = request

        self.data_store = data_store
        self.calibration = calibration
        self.tmp = os.getenv("TMP", default="/tmp")

        RosUtil.create_cv_brigde()

        self.bag = None
        self.bag_path = None
        self.bag_name = None

        self.multipart = 'multipart' in request['accept']

        if self.multipart:
            self.msg_count = 0

        self.accept = request['accept']
        if self.accept.startswith('s3/'):
            s3_config = self.data_store['s3']
            self.rosbag_bucket = s3_config['rosbag_bucket']
            self.rosbag_prefix = s3_config['rosbag_prefix']
            if not self.rosbag_prefix.endswith("/"):
                self.rosbag_prefix += "/"

        cal_obj = get_s3_resource().Object(calibration["cal_bucket"], calibration["cal_key"])
        cal_data = cal_obj.get()['Body'].read().decode('utf-8')
        self.cal_json = json.loads(cal_data) 

        self.__init_request(dbconfig)

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)

    def __init_request(self, dbconfig):
        self.manifests = dict() 
        sensors = self.request['sensor_id']
        self.sensor_dict = dict()
        self.sensor_list = list()
        self.sensor_active = dict()
        self.sensor_index = 0
        self.round_robin = list()
        self.sensor_data_type = dict()
        self.sensor_transform = dict()

        lidar_view = self.request.get("lidar_view", "camera")
        image_request = self.request.get("image", "original")
    
        for sensor in sensors:
            data_type = self.request["data_type"][sensor]
            self.sensor_data_type[sensor] = data_type
            self.manifests[sensor] = create_manifest(request=self.request, dbconfig=dbconfig, sensor_id=sensor)
            self.sensor_dict[sensor] = []
            self.sensor_list.append(sensor)
            self.sensor_active[sensor] = True

            if lidar_view == "vehicle" and ( data_type == RosUtil.PCL_DATA_TYPE or data_type == RosUtil.MARKER_ARRAY_CUBE_DATA_TYPE) :
                self.sensor_transform[sensor] = RosUtil.sensor_to_vehicle(cal_json=self.cal_json, sensor=sensor)
            elif  image_request == "undistorted" and data_type == RosUtil.IMAGE_DATA_TYPE:
                self.sensor_transform[sensor] = RosUtil.get_undistort_fn( cal_json=self.cal_json, sensor=sensor)

        if len(sensors) > 1:
            self.bag_lock = threading.Lock()
        else:
            self.bag_lock = None
        
        if self.multipart:
            self.chunk_count = len(self.sensor_list)*self.request.get("multipart_nmsgs", 2)

        max_rate = self.request.get("max_rate", 0)
        if max_rate > 0:
            self.sleep_interval = (len(self.sensor_list)/max_rate)
        else:
            self.sleep_interval  = 0

        self.ros_publishers = dict()


    def __create_bag_dir(self):
        if self.accept.startswith('s3/'):
            self.bag_dir = os.path.join(self.tmp, self.request['response_topic'])
        elif self.accept.startswith('fsx/'):
            fsx_config = self.data_store['fsx']
            self.bag_dir = os.path.join(fsx_config['rosbag'], self.request['response_topic'])
        elif self.accept.startswith('efs/'):
            efs_config = self.data_store['efs']
            self.bag_dir = os.path.join(efs_config['rosbag'], self.request['response_topic'])

        mkdir_p(self.bag_dir) 

    def __open_bag(self):
        if self.multipart:
            name = "input-{0}.bag".format(self.msg_count)
        else:
            name = "input.bag"

        self.bag_name = name
        self.bag_path = os.path.join(self.bag_dir, name)
        self.bag = rosbag.Bag(self.bag_path, 'w')

    def __close_bag(self, s3_client=None):
        if self.bag:
            self.bag.close()
            resp_topic = self.request['response_topic']
            if self.accept.startswith("s3/"):
                if s3_client == None:
                    s3_client = get_s3_client()
                prefix = self.rosbag_prefix + resp_topic + "/"
                key = prefix +  self.bag_name
                json_msg = { "output": "s3", "bag_bucket": self.rosbag_bucket, 
                        "bag_prefix": prefix, 
                        "bag_name": self.bag_name, "multipart": self.multipart} 
                with open(self.bag_path, 'rb') as data:
                    s3_client.upload_fileobj(data, self.rosbag_bucket, key)
                    data.close()
                    os.remove(self.bag_path)
            elif self.accept.startswith("fsx/"):
                json_msg = { "output": "fsx", "bag_path": self.bag_path , 
                    "multipart": self.multipart} 
            elif self.accept.startswith("efs/"):
                json_msg = { "output": "efs", "bag_path": self.bag_path , 
                    "multipart": self.multipart} 

            self.producer.send(resp_topic, json.dumps(json_msg).encode('utf-8'))

            self.bag = None
            self.bag_path = None

    def __write_ros_msg_to_bag(self, sensor=None, msg=None, s3_client=None, flush=False):

        if not self.bag:
            self.__open_bag()
        topic = self.request['ros_topic'][sensor]
        self.bag.write(topic, msg)

        if not flush and not self.__is_bus_sensor(sensor):
            self.round_robin.append(sensor)
            if self.multipart:
                self.msg_count += 1
                if self.msg_count % self.chunk_count == 0:
                    self.__close_bag(s3_client=s3_client)

    def __is_sensor_alive(self, sensor):
        return  self.sensor_dict[sensor] or self.sensor_active[sensor]
    
    def __round_robin_sensor(self,  sensor=None, msg=None):
        # add message to sensor queue
        self.sensor_dict[sensor].append(Qmsg(msg=msg))
                
        msg = None
        sensor = None

        # round robin through sensors
        _nsensors = len(self.sensor_list)
        for _ in self.sensor_list:
            self.sensor_index = (self.sensor_index + 1) % _nsensors
            _sensor = self.sensor_list[ self.sensor_index ]
            if _sensor in self.round_robin and any([True for k in self.sensor_active.keys() if k not in self.round_robin and self.__is_sensor_alive(k)]):
                continue

            if self.sensor_dict[_sensor]:
                front = self.sensor_dict[_sensor].pop(0)
                msg = front.msg
                sensor = _sensor
                break
        
        if self.sleep_interval > 0:
            time.sleep(self.sleep_interval)

        return sensor, msg

    def __flush_bag(self):
        try:
            if self.bag_lock:
                self.bag_lock.acquire()

            _nsensors = len(self.sensor_list)
              
            msg = None
            sensor = None

            _nsensors_flushed = 0
            # rotate through sensors and flush them to bag
            self.logger.info("Flushing  ROS sensors to bag")
            for _ in range(_nsensors):
                self.sensor_index = (self.sensor_index + 1) % _nsensors
                _sensor = self.sensor_list[ self.sensor_index ]
                
                if self.sensor_dict[_sensor]:
                    front = self.sensor_dict[_sensor].pop(0)
                    msg = front.msg
                    sensor = _sensor
                else:
                    _nsensors_flushed += 1

                if sensor and msg:
                    self.__write_ros_msg_to_bag(sensor=sensor, msg=msg, s3_client=None, flush=True)
                
                if _nsensors == _nsensors_flushed:
                    break
            self.logger.info("Flushed ROS sensors to bag")

        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))
            raise
        finally:    
            if self.bag_lock:
                self.bag_lock.release()
    
    def __is_bus_sensor(self, sensor=None):
        return self.request["data_type"][sensor] == RosUtil.BUS_DATA_TYPE

    def __record_sensor_data(self, sensor=None, ts=None, frame_id=None, ros_msg_fn=None, params=None, s3_client=None):
        try:
            ros_msg = ros_msg_fn(**params)
            RosUtil.set_ros_msg_header( ros_msg=ros_msg, ts=ts, frame_id=frame_id)

            if self.bag_lock:
                self.bag_lock.acquire()

            sensor, ros_msg = self.__round_robin_sensor(sensor=sensor, msg=ros_msg)
            if sensor and ros_msg:
                self.__write_ros_msg_to_bag(sensor=sensor, msg=ros_msg, s3_client=s3_client)

            sensors = [k for k in self.sensor_active.keys() if not self.__is_bus_sensor(k) and self.__is_sensor_alive(k)]
            if (len(self.round_robin) >= len(sensors)) and self.round_robin:
                self.round_robin.pop(0)
                
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))
            raise
        finally:
            if self.bag_lock:
                self.bag_lock.release()

    
    def __record_sensor_from_fs(self, manifest=None,  sensor=None, frame_id=None):
        data_loader = dict()
        data = dict() 
        ts = dict()

        data_type = self.sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        transform =   self.sensor_transform.get(sensor, None)

        while True:
            files = None
            while not files and manifest.is_open():
                files = manifest.fetch()
            if not files:
                break

            count = RosUtil.load_data_from_fs(data_type=data_type,
                data_store=self.data_store, data_files=files, data_loader=data_loader, data=data, ts=ts)
        
            for i in range(0, count):
                data_loader[i].join()
                try:
                    params = RosUtil.get_ros_msg_fn_params(data_type=data_type, data=data[i], 
                        sensor=sensor, request=self.request, transform=transform)
                    self.__record_sensor_data(sensor=sensor, ts=ts[i], frame_id=frame_id, ros_msg_fn=ros_msg_fn, params=params)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.logger.error(str(exc_type))
                    self.logger.error(str(exc_value))

            if self.request.get('preview', False):
                break

        self.sensor_active[sensor] = False

    def __record_sensor_from_s3(self, manifest=None, sensor=None, frame_id=None):

        s3_client = get_s3_client()
        req = Queue()
        resp = Queue()
        s3_reader = S3Reader(req, resp)
        s3_reader.start()

        data_type = self.sensor_data_type[sensor]
        ros_msg_fn = RosUtil.get_ros_msg_fn(data_type=data_type)
        data_load_fn = RosUtil.get_data_load_fn(data_type=data_type)
        transform = self.sensor_transform.get(sensor, None)

        while True:
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
                        data=data, sensor=sensor, request=self.request, transform=transform)
                    self.__record_sensor_data(sensor=sensor, ts=ts, frame_id=frame_id, 
                        ros_msg_fn=ros_msg_fn, params=params, s3_client=s3_client)

                    os.remove(path)
                except BaseException as _:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                    self.logger.error(str(exc_type))
                    self.logger.error(str(exc_value))

            if self.request.get('preview', False):
                break

        req.put("__close__")
        s3_reader.join(timeout=2)
        if s3_reader.is_alive():
            s3_reader.terminate()
        
        self.sensor_active[sensor] = False

    def __record_bus(self, manifest=None, sensor=None, frame_id=None):
        ros_msg_fn=RosUtil.get_ros_msg_fn(RosUtil.BUS_DATA_TYPE)
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
                    self.logger.error(str(exc_type))
                    self.logger.error(str(exc_value))

            if self.request.get('preview', False):
                break
        
        self.sensor_active[sensor] = False

    def __record_sensor(self, manifest=None, sensor=None, frame_id=None):
        try:
            data_type = self.request["data_type"][sensor]
            if data_type ==  RosUtil.BUS_DATA_TYPE:
                self.__record_bus(manifest=manifest, sensor=sensor, frame_id=frame_id)
            else:
                if self.data_store['input'] != 's3':
                    self.__record_sensor_from_fs(manifest=manifest, sensor=sensor, frame_id=frame_id)
                else:
                    self.__record_sensor_from_s3(manifest=manifest, sensor=sensor, frame_id=frame_id)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def run(self):

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.servers, 
                    client_id=random_string())

            self.__create_bag_dir()

            tasks = []

            sensors = self.request["sensor_id"]
            sensor_frame_id = self.request.get("frame_id", dict())

            for s in sensors:
                manifest = self.manifests[s]
                frame_id = sensor_frame_id.get(s, "map")
                t = threading.Thread(target=self.__record_sensor, name=s,
                    kwargs={"manifest": manifest,  "sensor":  s, "frame_id": frame_id})
                tasks.append(t)
                t.start()
                self.logger.info("Started thread:" + t.getName())

            for t in tasks:
                self.logger.info("Wait on thread:" + t.getName())
                t.join()
                self.logger.info("Thread finished:" + t.getName())

            self.logger.info("Flush ROS bag")
            self.__flush_bag()

            self.logger.info("Close ROS bag")
            self.__close_bag()

            json_msg = {"__close__": True} 
            resp_topic = self.request['response_topic']
            self.producer.send(resp_topic, json.dumps(json_msg).encode('utf-8'))

            self.producer.flush()
            self.producer.close()
            print("completed request:"+resp_topic)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))
        
    def __exit_gracefully(self, signum, frame):
    
        try:
            self.logger.error("\nReceived {} signal".format(self.signals[signum]))

            json_msg = {"__close__": True} 
            resp_topic = self.request['response_topic']
            self.producer.send(resp_topic, json.dumps(json_msg).encode('utf-8'))

            self.producer.flush()
            self.producer.close()
            self.logger.error("aborted request:"+resp_topic)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))