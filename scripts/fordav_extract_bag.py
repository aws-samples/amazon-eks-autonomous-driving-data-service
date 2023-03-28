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
import random

import subprocess
import sys, traceback
import time
from threading import Thread
import logging
from typing import Any
import boto3
import json
import os
import re
from multiprocessing import Process
from queue import Queue, Empty

from boto3.s3.transfer import TransferConfig

import numpy as np
import open3d as o3d
import ctypes
import struct
import glob
import string
import shutil

import rospy
import rosbag

from std_msgs.msg import Header
from sensor_msgs.msg import PointCloud2
import sensor_msgs.point_cloud2 as pc2
from sensor_msgs.msg import NavSatFix
from sensor_msgs.msg import Imu
from sensor_msgs.msg import TimeReference

from geometry_msgs.msg import PoseStamped
from geometry_msgs.msg import Vector3Stamped
from geometry_msgs.msg import Quaternion
from geometry_msgs.msg import Vector3
from tf.msg import tfMessage

class S3Uploader(Process):
    MAX_ATTEMPTS = 4

    def __init__(self, local_dir:str, bucket:str, prefix:str, vehicle_id:str, scene_id:str):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.__logger = logging.getLogger("s3_uploader")
        self.__local_dir = local_dir
        self.__bucket = bucket
        self.__prefix = prefix
        self.__vehicle_id = vehicle_id
        self.__scene_id = scene_id
        self.__s3_client = boto3.client("s3")
    
    def run(self):
        self.__upload_local_files()

    def __retry(self, attempt: int) -> int:
        if attempt <= self.MAX_ATTEMPTS:
            attempt += 1

        interval = random.uniform(2**(attempt - 1), 2**attempt)
        self.__logger.info(f"Will retry after: {interval} seconds")
        time.sleep(interval)
        return attempt

    def __upload_local_files(self):
        file_exts = ('*pcd') # file extension types
        files = []
        for file_ext in file_exts:
            files.extend(glob.glob(f"{self.__local_dir}/{file_ext}"))
       
        self.__logger.info(f"Number of local files to upload to S3: {len(files)}")
        count = 0
        for file in files:
            if not os.path.isfile(file):
                self.__logger.warning(f"Skipping upload, not a file: {file}")
                continue

            file_path = file.rsplit(os.sep, 2)
            sensor_id = file_path[-2]
            file_name = file_path[-1]

            key = f"{self.__prefix}{sensor_id}/vehicle_id={self.__vehicle_id}/scene_id={self.__scene_id}/{file_name}"
            self.__upload_s3(local_path=file, bucket=self.__bucket, key=key)
            
            count += 1
            if count % 1000 == 0:
                 self.__logger.info(f"File count uploaded to S3: {count}")

        self.__logger.info(f"Total file count uploaded to S3: {count}")

    def __upload_s3(self, local_path:str, bucket:str, key:str):
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

class DownloadProgress(Thread):

    def __init__(self, size:int, logger:logging.Logger):
        Thread.__init__(self)
        self._size = size
        self._seen_so_far = 0
        self._start = time.time()
        self._prev = 0.0
        self._perc = 0.0
        self.__logger = logger

    def run(self):
        last_seen = self._seen_so_far
        while self._perc < 100:
            time.sleep(120)
            if self._seen_so_far == last_seen:
                sys.exit("Abort download because it is not progressing for 120 seconds")
            else:
                last_seen = self._seen_so_far
            
    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        self._perc = round((self._seen_so_far / self._size) * 100, 2)
        if (self._perc - self._prev) > 5:
            self._prev = self._perc
            elapsed = time.time() - self._start
            self.__logger.info(f'percentage completed... {self._perc} in {elapsed} secs')


class FordavNode(Process):
 
    def __init__(self, node_name: str, sensor:str, output_dir:str):
        Process.__init__(self)
        
        logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.__logger = logging.getLogger(sensor)
        self.__queue = Queue()
        self.__sensor = sensor
        self.__sensor_dir = os.path.join(output_dir, self.__sensor)
        os.makedirs(self.__sensor_dir, mode=0o777, exist_ok=True)
        self.__node_name = node_name

    def __write_point_cloud2(self, ros_point_cloud: PointCloud2):

        ts = ros_point_cloud.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
        file_path = os.path.join(self.__sensor_dir, f"{ts_microsecs}.pcd")
        
        points_data = pc2.read_points(ros_point_cloud, skip_nans=True)
        points_list = list(points_data)

        xyz = np.empty((len(points_list), 3))
        rgb = np.empty((len(points_list), 3))

        for idx, p in enumerate(points_list):

            raw = p[3] 
            # cast float32 to int so that bitwise operations are possible
            s = struct.pack('>f' ,raw)
            i = struct.unpack('>l',s)[0]
            # you can get back the float value by the inverse operations
            pack = ctypes.c_uint32(i).value
            r = (pack & 0x00FF0000)>> 16
            g = (pack & 0x0000FF00)>> 8
            b = (pack & 0x000000FF)
            # x,y,z can be retrieved from the p[0],p[1],p[2]
            xyz[idx] = p[0:3]
            rgb[idx] = [r, g, b]

        out_pcd = o3d.geometry.PointCloud()    
        out_pcd.points = o3d.utility.Vector3dVector(xyz)
        out_pcd.colors = o3d.utility.Vector3dVector(rgb)
        o3d.io.write_point_cloud(file_path, out_pcd)
    

    def __write_ros_msgs(self):
        try:
            while msg := self.__queue.get(block=True, timeout=120):
                self.__write_point_cloud2(msg)
        except Empty:
            self.__logger.info(f"Exit node: {self.__sensor}")
            sys.exit()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))
            sys.exit(str(e))

    def run(self):

        t = Thread(target=self.__write_ros_msgs)
        t.start()

        self.__logger.info(f"Init node: {self.__node_name}")
        rospy.init_node(self.__node_name)
        self.__logger.info(f"Subscribe to topic: /{self.__sensor}")
        rospy.Subscriber(f"/{self.__sensor}", PointCloud2, self.__ros_cb)
        t.join()

    def __ros_cb(self, ros_point_cloud: PointCloud2):
        try:  
            self.__queue.put_nowait(ros_point_cloud)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

class FordavSensors:
    GPS = "gps"
    GPS_TIME = "gps_time"
    IMU = "imu"
    POSE_RAW = "pose_raw"
    POSE_LOCALIZED = "pose_localized"
    POSE_GROUND_TRUTH = "pose_ground_truth"
    VELOCITY_RAW = "velocity_raw"
    TF = "tf"

    @classmethod
    def get_sensors(cls):

        return [
            cls.GPS, 
            cls.GPS_TIME,
            cls.IMU,
            cls.POSE_GROUND_TRUTH,
            cls.POSE_LOCALIZED,
            cls.POSE_RAW,
            cls.VELOCITY_RAW,
            cls.TF
        ]


class FordavBagReader(Process):

    def __init__(self, vehicle_id:str, scene_id:str, ros_topic:str, bag_path:str, config:dict):
        super().__init__()
        
        logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.__logger = logging.getLogger("fordav-bag")
        
        self.__vehicle_id = vehicle_id
        self.__scene_id = scene_id


        self.__ros_topic = ros_topic
        self.__bag_path = bag_path

        dest_prefix = config['dest_prefix']
        config['dest_prefix'] = dest_prefix if dest_prefix.endswith("/") else f"{dest_prefix}/"
        self.__config = config

        self.__s3_client = boto3.client("s3")
        
        self.__fns = dict()
        self.__fns[f"/{FordavSensors.POSE_RAW}"] = self.__pose_raw_fn
        self.__fns[f"/{FordavSensors.POSE_LOCALIZED}"] = self.__pose_localized_fn
        self.__fns[f"/{FordavSensors.POSE_GROUND_TRUTH}"] = self.__pose_ground_truth_fn
        self.__fns[f"/{FordavSensors.GPS}"] = self.__gps_fn
        self.__fns[f"/{FordavSensors.GPS_TIME}"] = self.__gps_time_fn
        self.__fns[f"/{FordavSensors.IMU}"] = self.__imu_fn
        self.__fns[f"/{FordavSensors.VELOCITY_RAW}"] = self.__velocity_raw_fn
        self.__fns[f"/{FordavSensors.TF}"] = self.__tf_fn

        self.__msg_count = dict()
        
    @staticmethod
    def header(header: Header) -> dict:

        data = { 
            "seq": header.seq, 
            "stamp":  {
                "secs": header.stamp.secs,
                "nsecs": header.stamp.nsecs
            },
            "frame_id": header.frame_id
        }
    
        return data

    @staticmethod
    def quaternion(q: Quaternion) -> dict:
        data = {
            "x": q.x,
            "y": q.y,
            "z": q.z,
            "w": q.w
        }
    
        return data
    
    @staticmethod
    def vector3(v: Vector3) -> dict:
        data = {
            "x": v.x,
            "y": v.y,
            "z": v.z
        }
    
        return data

    def __put_object(self, json_obj:dict, sensor_id: str, obj_name:str):
        """Put json object in S3 bucket
        """

        prefix = self.__config['dest_prefix']
        bucket = self.__config['dest_bucket']
        
        key = f"{prefix}{sensor_id}/vehicle_id={self.__vehicle_id}/scene_id={self.__scene_id}/{obj_name}"
        attempt = 0
       
        while True:
            try:
                self.__s3_client.put_object(Body=json.dumps(json_obj), Bucket=bucket, Key=key)
                break
            except Exception as error:
                self.__logger.warning(f"Upload error: {key}, {error}")
                attempt = self.__retry(attempt=attempt)

    def __increment_msg_count(self, ros_topic) -> int:
        msg_count = self.__msg_count.get(ros_topic, 0)
        msg_count += 1
        self.__msg_count[ros_topic] = msg_count
        return msg_count
    
    def __write_tf_message(self, msg: tfMessage, sensor:  str):

        transforms = msg.transforms
        transform_stamped = transforms[0]

        ts = transform_stamped.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
        
        transform = transform_stamped.transform
        data = { 
            "transforms": [
                {
                    "header": self.header(transform_stamped.header),
                    "child_frame_id": transform_stamped.child_frame_id,
                    "transform": {
                        "translation": self.vector3(transform.translation),
                        "rotation": self.quaternion(transform.rotation)
                    }
                }
            ]

        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    def __write_time_reference(self, msg: TimeReference, sensor:  str):

        ts = msg.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
        
        time_ref = msg.time_ref
        data = { 
            "header": self.header(msg.header),
            "time_ref": {
                "secs": time_ref.secs,
                "nsecs": time_ref.nsecs
            },
            "source": msg.source
        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    def __write_imu(self, msg: Imu, sensor:  str):

        ts = msg.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
       
        orientation = msg.orientation
        angular_velocity = msg.angular_velocity
        linear_acceleration = msg.linear_acceleration

        data = { 
            "header": self.header(msg.header),
            "orientation": self.quaternion(orientation),
            "orientation_covariance": msg.orientation_covariance,
            "angular_velocity": self.vector3(angular_velocity),
            "angular_velocity_covariance": msg.angular_velocity_covariance,
            "linear_acceleration": self.vector3(linear_acceleration),
            "linear_acceleration_covariance": msg.linear_acceleration_covariance
        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    def __write_vector3_stamped(self, msg: Vector3Stamped, sensor:  str):

        ts = msg.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
       
        vector = msg.vector
        data = { 
            "header": self.header(msg.header),
            "vector": {
                "x": vector.x,
                "y": vector.y,
                "z": vector.z
            }
        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    def __write_navsat_fix(self, msg: NavSatFix, sensor:  str):

        ts = msg.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
       
        status = msg.status
        data = { 
            "header": self.header(msg.header),
            "status": {
                "status": status.status,
                "service": status.service
            },
            "latitude": msg.latitude,
            "longitude": msg.longitude,
            "altitude": msg.altitude,
            "position_covariance": msg.position_covariance,
            "position_covariance_type": msg.position_covariance_type
        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    def __write_pose_stamped(self, msg: PoseStamped, sensor:  str):

        ts = msg.header.stamp
        ts_microsecs = int(ts.secs*10**6 + (ts.nsecs/1000))
        
        position = msg.pose.position
        orientation = msg.pose.orientation

        data = { 
            "header": self.header(msg.header),
            "pose": {
                "position": {
                    "x": position.x,
                    "y": position.y,
                    "z": position.z
                },
                "orientation": self.quaternion(orientation)
            }
        }

        self.__put_object(json_obj=data, sensor_id=sensor, obj_name=f"{ts_microsecs}.json")

    
    def run(self):
        start = time.time()
          
        self.__logger.info(f"Rosbag reader: {self.__bag_path}; start: {start}")
        reader = rosbag.Bag(self.__bag_path)
        for ros_topic, ros_msg, _ in reader.read_messages():

            if ros_topic != self.__ros_topic:
                continue

            msg_count = self.__increment_msg_count(ros_topic=ros_topic)
            
            fn = self.__fns.get(ros_topic, None)
            if fn:
                fn(ros_msg)

            if msg_count % 100 == 0:
                self.__logger.info(f"Messages {ros_topic}: {msg_count}")
        
        self.__logger.info(f"Total messages: {self.__msg_count}")
        self.__logger.info(f"Rosbag end,  elapsed: {time.time() - start} secs")
        reader.close()

    def __velocity_raw_fn(self, v: Vector3Stamped):
        try:  
            self.__write_vector3_stamped(v, FordavSensors.VELOCITY_RAW)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __tf_fn(self, tf: tfMessage):
        try:  
            self.__write_tf_message(tf, FordavSensors.TF)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __imu_fn(self, imu: Imu):
        try:  
            self.__write_imu(imu, FordavSensors.IMU)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __gps_time_fn(self, gps_time: TimeReference):
        try:  
            self.__write_time_reference(gps_time, FordavSensors.GPS_TIME)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __gps_fn(self, gps: NavSatFix):
        try:  
            self.__write_navsat_fix(gps, FordavSensors.GPS)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __pose_ground_truth_fn(self, pose: PoseStamped):
        try:  
            self.__write_pose_stamped(pose, FordavSensors.POSE_GROUND_TRUTH)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __pose_localized_fn(self, pose: PoseStamped):
        try:  
            self.__write_pose_stamped(pose, FordavSensors.POSE_LOCALIZED)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __pose_raw_fn(self, pose: PoseStamped):
        try:  
            self.__write_pose_stamped(pose, FordavSensors.POSE_RAW)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

class FordavBag(object):
    GB = 1024**3
    S3_MAX_IO_QUEUE = 1000
    S3_IO_CHUNKSIZE = 262144
    MAX_ATTEMPTS = 4
     
    def __init__(self, config:dict):
        super().__init__()
        
        logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.__logger = logging.getLogger("fordav-bag")
        
        dest_prefix = config['dest_prefix']
        config['dest_prefix'] = dest_prefix if dest_prefix.endswith("/") else f"{dest_prefix}/"
        self.__config = config

        self.__s3_client = boto3.client("s3")
        self.__batch_client = boto3.client("batch")

    
    def __submit_job(self, job_name: str, lidar: str) -> Any:
    
        job_queue = self.__config["job_queue"]
        s3_json_config = self.__config["s3_json_config"]
        s3_python_script = self.__config["bag_s3_python_script"]
        job_definition = self.__config["ros1_job_definition"]
        
        aws_region = os.environ['AWS_DEFAULT_REGION']
        command =  ['--bag-path', self.__bag_path, 
                    "--output-dir", self.__output_dir,
                    "--lidar", lidar]

        return self.__batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            retryStrategy={'attempts': 5},
            timeout={'attemptDurationSeconds': 86400},
            containerOverrides={
                'command': command,
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

    def __extract_lidar_pointcloud(self):
        self.__jobs={}
        
        lidars = [ "red", "yellow", "green", "blue"]
        for lidar in lidars:
            job_name = f"fordav-lidar_{lidar}_pointcloud-{str(time.time()).replace('.','-')}"
            response = self.__submit_job(job_name=job_name, lidar=lidar)
            
            jobId = response["jobId"]
            self.__jobs[jobId] = job_name

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
        
        self.__cleanup()
        sys.exit(reason)

    def __s3_download_file(self, bucket_name=None, key=None):
        file_name = key if key.find('/') == -1 else key.rsplit('/', 1)[1]
        file_path = os.path.join(self.__tmp_dir, file_name)
            
        with open(file_path, 'wb') as data:
            start_time = time.time()
                
            file_size = self.__s3_client.head_object(Bucket=bucket_name, Key=key).get('ContentLength')
            self.__logger.info(f"Begin download: s3://{bucket_name}/{key} : {file_size} bytes")
            transfer_config = TransferConfig(multipart_threshold=self.GB, 
                    multipart_chunksize=self.GB,
                    max_io_queue=self.S3_MAX_IO_QUEUE, 
                    io_chunksize=self.S3_IO_CHUNKSIZE)
            try:
                _download_callback = DownloadProgress(file_size, self.__logger)
                _download_callback.start()
                self.__s3_client.download_fileobj(bucket_name, key, data, Config=transfer_config, Callback=_download_callback)
            except Exception as e:
                self.__logger.error(f"File download error: {e} ")
                sys.exit(1)
            finally:
                data.close()
                
            elapsed = time.time() - start_time
            file_size = os.stat(file_path).st_size
            self.__logger.info(f"Download completed: {file_path}: {file_size} bytes in {elapsed} secs")

        return file_path

    def __play_rosbag(self):
        start = time.time()
        self.__logger.info(f"Rosbag play begin: {self.__bag_path}; output: {self.__output_dir}, start: {start}")
        exit_code = subprocess.check_call(["rosbag", "play", self.__bag_path], 
                                        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        self.__logger.info(f"Rosbag play end, exit code: {exit_code}, elapsed: {time.time() - start} secs")
    
    def __download_rosbag(self):
        bucket = self.__config["source_bucket"]
        key = self.__config["key"]

        key_parts = key.rsplit("/", 1)
        bag_name = key_parts[1] if len(key_parts) > 1 else key_parts[0]

        m = re.match("(\d\d\d\d-\d\d-\d\d)-(V\d+)-(Log\d+)\.bag", bag_name)
        self.__vehicle_id = m[2]
        self.__scene_id = f"{m[1]}-{m[3]}"

        self.__output_dir = os.path.join(self.__tmp_dir, "fordav", self.__vehicle_id, self.__scene_id)
        os.makedirs(self.__output_dir, mode=0o777, exist_ok=True)

        self.__logger.info(f"Bucket: {bucket}, Key: {key}")
        self.__bag_path = self.__s3_download_file(bucket_name=bucket, key=key)
        self.__logger.info(f"Downloaded {self.__bag_path}")

    def __s3_upload_data(self):

        bucket = self.__config['dest_bucket']
        prefix = self.__config['dest_prefix']

        self.__logger.info(f"Upload data: s3://{bucket}/{prefix}/")

        local_dirs = [f.path for f in os.scandir(self.__output_dir) if f.is_dir()]
        jobs = []
        for local_dir in local_dirs:
            job = S3Uploader(local_dir=local_dir,
                            bucket=bucket, 
                            prefix=prefix,
                            vehicle_id=self.__vehicle_id,
                            scene_id=self.__scene_id)
            jobs.append(job)
            job.start()

        for job in jobs:
            job.join()

    def __call__(self):

        key = self.__config['key']
        if key:
            tmp_dir = config.get("tmp_dir", "/efs/tmp")
            random_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))
            self.__tmp_dir = os.path.join(tmp_dir, random_name)
            os.makedirs(self.__tmp_dir, mode=0o777, exist_ok=True)

            self.__download_rosbag()
            t = Thread(target=self.__extract_lidar_pointcloud)
            t.start()

            readers = []
            sensors = FordavSensors.get_sensors()
            for sensor in sensors:
                reader = FordavBagReader(vehicle_id=self.__vehicle_id,
                                         scene_id = self.__scene_id, ros_topic=f"/{sensor}", 
                                         bag_path=self.__bag_path, 
                                         config=self.__config)
                readers.append(reader)
                reader.start()

            t.join()

            for reader in readers:
                reader.join()

            self.__s3_upload_data()
            self.__cleanup()
        else:
            lidar = self.__config["lidar"]
            self.__bag_path = self.__config['bag_path']
            self.__output_dir = self.__config['output_dir']
            ros_package_name = self.__config["ros_package_name"]
            ros_package_launch = self.__config["ros_package_launch"]
            subprocess.Popen(["roslaunch", ros_package_name, ros_package_launch])

            lidar_sensor = f"lidar_{lidar}_pointcloud"
            node_name = f"fordav_node"
            self.__logger.info(f"Start node: {node_name}")
            node = FordavNode(node_name=node_name, sensor=lidar_sensor, output_dir=self.__output_dir)
            node.start()
            
            self.__play_rosbag()
            node.join()

    def __cleanup(self):
        try:
            self.__logger.info(f"cleanup: {self.__tmp_dir}")
            shutil.rmtree(self.__tmp_dir, ignore_errors=True)
        except Exception as e:
            self.__logger.warning(str(e))

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract data from Ros bag')
    parser.add_argument('--config', type=str,  help='Configuration JSON file', required=True)
    parser.add_argument('--key', type=str,  default='', help='S3 key for the bag file', required=False)
    parser.add_argument('--bag-path', type=str,  default='', help='Ros bag path', required=False)
    parser.add_argument('--output-dir', type=str,  default='', help='Output directory', required=False)
    parser.add_argument('--lidar', type=str,  default='', 
                        choices=["red", "blue", "yellow", "green"],
                        help='Lidar', required=False)

    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    config['key'] = args.key
    config['bag_path'] = args.bag_path
    config['lidar'] = args.lidar
    config['output_dir'] = args.output_dir
    
    assert config['key'] or ( os.path.isfile(config['bag_path']) and os.path.isdir(config['output_dir']) and config['lidar'])

    fordav_bag = FordavBag(config=config)
    fordav_bag()
    sys.exit()
        
        
        