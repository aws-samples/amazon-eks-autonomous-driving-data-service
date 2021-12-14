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
import numpy as np

from sensor_msgs.msg import PointCloud2, PointField
from a2d2_msgs.msg import Bus
import rosbag

def get_s3_client():
    s3_client = None
    try:
        session = boto3.session.Session()
        s3_client = session.client('s3')
    except Exception as e:
        try:
            print(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])
            print(os.environ['AWS_ROLE_ARN'])
            s3_client = boto3.client('s3')
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))

    assert(s3_client != None)
    return s3_client

def get_s3_resource():
    s3_resource = None
    try:
        session = boto3.session.Session()
        s3_resource = session.resource('s3')
    except Exception as e:
        try:
            print(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])
            print(os.environ['AWS_ROLE_ARN'])
            s3_resource = boto3.resource('s3')
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))

    return s3_resource

def random_string(length=16):
    s = ''
    sel = string.ascii_lowercase + string.ascii_uppercase + string.digits
    for _ in range(0, length):
        s += random.choice(sel)
    return s

def get_data_class(data_type):

    data_class = None
    if data_type == 'sensor_msgs/Image':
        from sensor_msgs.msg import Image
        data_class = Image
    elif data_type == 'sensor_msgs/PointCloud2':
        from sensor_msgs.msg import PointCloud2 
        data_class = PointCloud2
    elif data_type == 'a2d2_msgs/Bus':
        from a2d2_msgs.msg import Bus
        data_class = Bus
    else:
        raise ValueError("Data type not supported:" + str(data_type))

    return data_class

def get_topics_types(bag_path):

    bag = rosbag.Bag(bag_path)
    topics = bag.get_type_and_topic_info()[1] 
    topic_types = dict() 

    for topic, topic_tuple in topics.items():
        topic_types[topic] = topic_tuple[0]

    return topic_types

def is_close_msg(json_msg):
    close = False

    try:
        close = json_msg['__close__']
    except KeyError:
        pass

    return close

def bus_msg(row=None):
    msg = Bus()

    ts = row[2]
    _stamp = divmod(ts, 1000000 ) #  time stamp is in micro secs
    msg.header.stamp.secs = _stamp[0] # secs
    msg.header.stamp.nsecs = _stamp[1]*1000 # nano secs

    # linear accel
    msg.vehicle_kinematics.acceleration.x = row[3]
    msg.vehicle_kinematics.acceleration.y = row[4]
    msg.vehicle_kinematics.acceleration.z = row[5]

    # accelerator control
    msg.control.accelerator_pedal = row[6]
    msg.control.accelerator_pedal_gradient_sign = row[7]

    # angular velocity
    msg.vehicle_kinematics.angular_velocity.omega_x = row[8]
    msg.vehicle_kinematics.angular_velocity.omega_y = row[9]
    msg.vehicle_kinematics.angular_velocity.omega_z = row[10]

    # brake pressure
    msg.control.brake_pressure = row[11]

    # distance pulse
    msg.distance_pulse.front_left = row[12]
    msg.distance_pulse.front_right = row[13]
    msg.distance_pulse.rear_left = row[14]
    msg.distance_pulse.rear_right = row[15]

    # geo location
    msg.geo_loction.latitude = row[16]
    msg.geo_loction.latitude_direction = row[17]
    msg.geo_loction.longitude = row[18]
    msg.geo_loction.longitude_direction = row[19]

    # angular orientation
    msg.vehicle_kinematics.angular_orientation.pitch_angle = row[20]
    msg.vehicle_kinematics.angular_orientation.roll_angle = row[21]

    # steering 
    msg.control.steeering_angle_calculated = row[22]
    msg.control.steering_angle_calculated_sign = row[23]

    # vehicle speed
    msg.vehicle_kinematics.vehicle_speed = row[24]

    return msg


def ros_pcl2_sparse(points=None, reflectance=None, rows=None, cols=None, ts=None, frame_id=None):
  
    rows = (rows + 0.5).astype(np.int)
    height= np.amax(rows) + 1
    
    cols = (cols + 0.5).astype(np.int)
    width=np.amax(cols) + 1

    colors = np.stack([reflectance, reflectance, reflectance], axis=1)
    pca = np.full((height, width, 3), np.inf)
    ca =np.full((height, width, 3), np.inf)
    assert(pca.shape == ca.shape)

    count = points.shape[0]
    for i in range(0, count):
        pca[rows[i], cols[i], :] = points[i]
        ca[rows[i], cols[i], : ] = colors[i]
        
    msg = PointCloud2()
    
    msg.header.frame_id = frame_id

    _stamp = divmod(ts, 1000000 ) #  time stamp is in micro secs
    msg.header.stamp.secs = _stamp[0] # secs
    msg.header.stamp.nsecs = _stamp[1]*1000 # nano secs
    
    msg.width = width
    msg.height = height
    
    msg.fields = [
        PointField('x', 0, PointField.FLOAT32, 1),
        PointField('y', 4, PointField.FLOAT32, 1),
        PointField('z', 8, PointField.FLOAT32, 1),
        PointField('r', 12, PointField.FLOAT32, 1),
        PointField('g', 16, PointField.FLOAT32, 1),
        PointField('b', 20, PointField.FLOAT32, 1)
    ]

    msg.is_bigendian = False
    msg.point_step = 24
    msg.row_step = msg.point_step * width
    msg.is_dense = False
    data_array = np.array(np.hstack([pca, ca]), dtype=np.float32)
    msg.data = data_array.tostring()

    return msg

def ros_pcl2_dense(points=None, reflectance=None, ts=None, frame_id=None):
  
    colors = np.stack([reflectance, reflectance, reflectance], axis=1)
    assert(points.shape == colors.shape)
   
    msg = PointCloud2()
    
    msg.header.frame_id = frame_id

    _stamp = divmod(ts, 1000000 ) #  time stamp is in micro secs
    msg.header.stamp.secs = _stamp[0] # secs
    msg.header.stamp.nsecs = _stamp[1]*1000 # nano secs
    
    msg.width = points.shape[0]
    msg.height = 1
    
    msg.fields = [
        PointField('x', 0, PointField.FLOAT32, 1),
        PointField('y', 4, PointField.FLOAT32, 1),
        PointField('z', 8, PointField.FLOAT32, 1),
        PointField('r', 12, PointField.FLOAT32, 1),
        PointField('g', 16, PointField.FLOAT32, 1),
        PointField('b', 20, PointField.FLOAT32, 1)
    ]

    msg.is_bigendian = False
    msg.point_step = 24
    msg.row_step = msg.point_step * msg.width
    msg.is_dense = True
    data_array = np.array(np.hstack([points, colors]), dtype=np.float32)
    msg.data = data_array.tostring()

    return msg

def mkdir_p(path):
    try:
        os.makedirs(path)
        os.chmod(path, stat.S_IROTH|stat.S_IWOTH|stat.S_IXOTH|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
