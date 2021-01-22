
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
import logging, time
import json
import random
import string
import cv2
import os
import threading
import boto3
import boto3.session
import numpy as np

import cv_bridge
import rosbag

from sensor_msgs.msg import Image

from kafka import KafkaProducer
from manifest_dataset import ManifestDataset
from util import random_string, get_s3_resource, get_s3_client, npz_pcl_dense, mkdir_p
from s3_reader import S3Reader

class Qmsg:
    def __init__(self, msg=None, ts=None):
        self.msg = msg
        self.ts = ts

class RosbagProducer(Process):
    def __init__(self, dbconfig=None, servers=None,
                request=None, data_store=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("rosbag_producer")

        self.servers = servers
        self.request = request
        self.data_store = data_store
        self.tmp = os.getenv("TMP", default="/tmp")

        self.img_cv_bridge = cv_bridge.CvBridge()

        self.manifests = dict() 
        self.last_topic = None
        sensors = self.request['sensor_id']
        self.topic_queue = dict()
        for s in sensors:
            self.manifests[s] = self.create_manifest(dbconfig=dbconfig, sensor_id=s)
            self.topic_queue[self.request['ros_topic'][s]] = []

        if len(sensors) > 1:
            self.bag_lock = threading.Lock()
        else:
            self.bag_lock = None

        self.bag = None
        self.bag_path = None
        self.bag_name = None

        self.multipart = 'multipart' in request['accept']

        if self.multipart:
            self.msg_count = 0

        self.accept = request['accept']
        if self.accept.startswith('s3/'):
            s3_config = self.data_store['s3']
            if self.multipart:
                self.chunk_count = 2 
            self.rosbag_bucket = s3_config['rosbag_bucket']
            self.rosbag_prefix = s3_config['rosbag_prefix']
            if not self.rosbag_prefix.endswith("/"):
                self.rosbag_prefix += "/"
        elif self.accept.startswith('efs/'):
            if self.multipart:
                self.chunk_count = 4 
        elif self.accept.startswith('fsx/'):
            if self.multipart:
                self.chunk_count = 6 


    def create_manifest(self, dbconfig=None, sensor_id=None):
        manifest = ManifestDataset(dbconfig=dbconfig, 
		vehicle_id=self.request["vehicle_id"],
		scene_id=self.request["scene_id"],
		sensor_id=sensor_id,
		start_ts=int(self.request["start_ts"]), 
		stop_ts=int(self.request["stop_ts"]),
                step=int(self.request["step"]))

        return manifest

    def fsx_read_image(self, id=None, img_path=None, image_data=None):
        ''' read image file from fsx file system '''
        fsx_config = self.data_store['fsx']
        fsx_root = fsx_config['root']
        image_data[id] = cv2.imread(os.path.join(fsx_root, img_path))
    
    
    def fsx_read_pcl(self, id=None, pcl_path=None, npz=None):
        ''' read pcl file from fsx file system '''
        fsx_config = self.data_store['fsx']
        fsx_root = fsx_config['root']
        pcl_path = os.path.join(fsx_root, pcl_path)
        npz[id] =  np.load(pcl_path)
        
    def efs_read_image(self, id=None, img_path=None, image_data=None):
        ''' read image file from efs file system '''
        efs_config = self.data_store['efs']
        efs_root = efs_config['root']
        image_data[id] = cv2.imread(os.path.join(efs_root, img_path))
    
    
    def efs_read_pcl(self, id=None, pcl_path=None, npz=None):
        ''' read pcl file from efs file system '''
        efs_config = self.data_store['efs']
        efs_root = efs_config['root']
        pcl_path = os.path.join(efs_root, pcl_path)
        npz[id] =  np.load(pcl_path)

    def create_bag_dir(self):
        if self.accept.startswith('s3/'):
            self.bag_dir = os.path.join(self.tmp, self.request['response_topic'])
        elif self.accept.startswith('fsx/'):
            fsx_config = self.data_store['fsx']
            self.bag_dir = os.path.join(fsx_config['rosbag'], self.request['response_topic'])
        elif self.accept.startswith('efs/'):
            efs_config = self.data_store['efs']
            self.bag_dir = os.path.join(efs_config['rosbag'], self.request['response_topic'])

        mkdir_p(self.bag_dir) 

    def open_bag(self):
        if self.multipart:
            name = "input-{0}.bag".format(self.msg_count)
        else:
            name = "input.bag"

        self.bag_name = name
        self.bag_path = os.path.join(self.bag_dir, name)
        self.bag = rosbag.Bag(self.bag_path, 'w')

    def close_bag(self, s3_client=None):
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

    def write_bag(self, topic, msg, ts, s3_client=None):
        try:
            if self.bag_lock:
                self.bag_lock.acquire()
                if topic == self.last_topic:
                    self.topic_queue[topic].append(Qmsg(msg=msg, ts=ts))
                    msg = None
                    ts = None
                    for k in self.topic_queue.keys():
                        if k != topic and len(self.topic_queue[k]) > 0:
                            topic = k
                            front = self.topic_queue[k].pop(0)
                            msg = front.msg
                            ts = front.ts
                            break
                    if not msg:
                        return

                else:
                    if len(self.topic_queue[topic]) > 0:
                        self.topic_queue[topic].append(Qmsg(msg=msg, ts=ts))
                        front = self.topic_queue[topic].pop(0)
                        msg = front.msg
                        ts = front.ts

            if not self.bag:
                self.open_bag()
            self.last_topic = topic
            self.bag.write(topic, msg, ts)
            if self.multipart:
                self.msg_count += 1
                if self.msg_count % self.chunk_count == 0:
                    self.close_bag(s3_client=s3_client)
        except Exception as e:
            print("write bag exception")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(str(e))
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            raise
        finally:
            if self.bag_lock:
                self.bag_lock.release()

    def s3_bag_images(self, manifest=None,  ros_topic=None):

        s3_client = get_s3_client()

        req = Queue()
        resp = Queue()

        s3_reader = S3Reader(req, resp)
        s3_reader.start()

        while True:
            files = manifest.fetch()
            if not files:
                break

            for f in files:
                bucket = f[0]
                key = f[1]
                req.put(bucket+" "+key)

            for f in files:
                path = resp.get(block=True).split(" ", 1)[0]
                image_data = cv2.imread(path)
                image_ts = int(f[2])
                ros_image_msg = self.img_cv_bridge.cv2_to_imgmsg(image_data)
                ros_image_msg.header.stamp.secs = divmod(image_ts, 1000000 )[0] #stamp in micro secs
                ros_image_msg.header.stamp.nsecs = divmod(image_ts, 1000000 )[1]*1000 # nano secs
                self.write_bag(ros_topic, ros_image_msg, ros_image_msg.header.stamp, s3_client=s3_client)
                os.remove(path)
                if self.bag_lock:
                    factor = len(self.topic_queue[ros_topic]) + 1
                    time.sleep(.000001*factor)

            if self.request['preview']:
                break

        req.put("__close__")
        s3_reader.join(timeout=2)
        if s3_reader.is_alive():
            s3_reader.terminate()

    def bag_images(self, manifest=None,  ros_topic=None):

        if self.data_store['input'] == 's3':
            self.s3_bag_images(manifest=manifest, ros_topic=ros_topic)
            return

        image_data = dict() 
        image_reader = dict() 
        image_ts = dict()

        while True:
            files = manifest.fetch()
            if not files:
                break

            image_data.clear()
            image_reader.clear() 
            image_ts.clear()

            idx = 0
            for f in files:
                if self.data_store['input'] == 'fsx':
                    img_path = f[1]
                    image_reader[idx] = threading.Thread(target=self.fsx_read_image, 
                            kwargs={"id": idx, "img_path": img_path, "image_data": image_data})
                elif self.data_store['input'] == 'efs':
                    img_path = f[1]
                    image_reader[idx] = threading.Thread(target=self.efs_read_image, 
                            kwargs={"id": idx, "img_path": img_path, "image_data": image_data})

                image_reader[idx].start()
                image_ts[idx]= int(f[2])
                idx += 1

            count = idx
            for i in range(0, count):
                image_reader[i].join()
                ros_image_msg = self.img_cv_bridge.cv2_to_imgmsg(image_data[i])
                ros_image_msg.header.stamp.secs = divmod(image_ts[i], 1000000 )[0] #stamp in micro secs
                ros_image_msg.header.stamp.nsecs = divmod(image_ts[i], 1000000 )[1]*1000 # nano secs
                self.write_bag(ros_topic, ros_image_msg, ros_image_msg.header.stamp)
                if self.bag_lock:
                    factor = len(self.topic_queue[ros_topic]) + 1
                    time.sleep(.000001*factor)

            if self.request['preview']:
                break


    def s3_bag_pcl(self, manifest=None,  ros_topic=None):

        s3_client = get_s3_client()

        req = Queue()
        resp = Queue()

        s3_reader = S3Reader(req, resp)
        s3_reader.start()

        while True:
            files = manifest.fetch()
            if not files:
                break

            for f in files:
                bucket = f[0]
                key = f[1]
                req.put(bucket+" "+key)

            for f in files:
                path = resp.get(block=True).split(" ", 1)[0]
                npz = np.load(path)
                pcl_ts= int(f[2])
                ros_pcl_msg = npz_pcl_dense(npz=npz,ts=pcl_ts,frame_id="map")
                self.write_bag(ros_topic, ros_pcl_msg, ros_pcl_msg.header.stamp, s3_client=s3_client)
                os.remove(path)
                if self.bag_lock:
                    factor = len(self.topic_queue[ros_topic]) + 1
                    time.sleep(.000001*factor)

            if self.request['preview']:
                break

        req.put("__close__")
        s3_reader.join(timeout=2)
        if s3_reader.is_alive():
            s3_reader.terminate()

    def bag_pcl(self, manifest=None,  ros_topic=None):

        if self.data_store['input'] == 's3':
            self.s3_bag_pcl(manifest=manifest, ros_topic=ros_topic)
            return

        pcl_reader = dict() 
        pcl_ts = dict()
        npz = dict()
        
        while True:
            files = manifest.fetch()
            if not files:
                break

            pcl_reader.clear()
            pcl_ts.clear()
            npz.clear() 

            idx = 0
            for f in files:
                if self.data_store['input'] == 'fsx':
                    pcl_path = f[1]
                    pcl_reader[idx] = threading.Thread(target=self.fsx_read_pcl, 
                            kwargs={"id": idx, "pcl_path": pcl_path, 
                                    "npz": npz})
                elif self.data_store['input'] == 'efs':
                    pcl_path = f[1]
                    pcl_reader[idx] = threading.Thread(target=self.efs_read_pcl, 
                            kwargs={"id": idx, "pcl_path": pcl_path, 
                                    "npz": npz})

                pcl_reader[idx].start()
                pcl_ts[idx]= int(f[2])
                idx += 1

            count = idx
            for i in range(0, count):
                pcl_reader[i].join()
                ros_pcl_msg = npz_pcl_dense(npz=npz[i],ts=pcl_ts[i],frame_id="map")
                self.write_bag(ros_topic, ros_pcl_msg, ros_pcl_msg.header.stamp)
                if self.bag_lock:
                    factor = len(self.topic_queue[ros_topic]) + 1
                    time.sleep(.000001*factor)

            if self.request['preview']:
                break

    
    def bag_data(self, manifest=None, data_type=None, ros_topic=None):
        try:
            if data_type ==  'sensor_msgs/Image':
                self.bag_images(manifest=manifest, ros_topic=ros_topic)
            elif data_type == 'sensor_msgs/PointCloud2':
                self.bag_pcl(manifest=manifest, ros_topic=ros_topic)
        except Exception as e:
            print("bag data exception")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(str(e))
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
        

    def run(self):

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.servers, 
                    client_id=random_string())

            self.create_bag_dir()

            tasks = []

            sensors = self.request["sensor_id"]
            for s in sensors:
                manifest = self.manifests[s]
                data_type = self.request["data_type"][s]
                ros_topic = self.request['ros_topic'][s]
                t = threading.Thread(target=self.bag_data, 
                    kwargs={"manifest": manifest, "data_type": data_type, 
                            "ros_topic": ros_topic })
                tasks.append(t)
                t.start()

            for t in tasks:
                t.join()

            self.close_bag()

            json_msg = {"__close__": True} 
            resp_topic = self.request['response_topic']
            self.producer.send(resp_topic, json.dumps(json_msg).encode('utf-8'))

            self.producer.flush()
            self.producer.close()
            print("completed request:"+resp_topic)
        except Exception as e:
            print("run exception")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(str(e))
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
        
