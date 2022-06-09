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

import time
import numpy as np
import cv2
import threading
import os
import util
import rospy

import cv_bridge

from sensor_msgs.msg import Image, PointCloud2, PointField
from a2d2_msgs.msg import Bus
from visualization_msgs.msg import  Marker, MarkerArray
from geometry_msgs.msg import  Pose
from  pyquaternion import Quaternion
from std_msgs.msg import ColorRGBA

from view import transform_from_to

class RosUtil(object):
    BUS_DATA_TYPE = 'a2d2_msgs/Bus'
    PCL_DATA_TYPE = "sensor_msgs/PointCloud2"
    IMAGE_DATA_TYPE = "sensor_msgs/Image"
    MARKER_ARRAY_CUBE_DATA_TYPE = "visualization_msgs/MarkerArray/Marker/CUBE"
    MARKER_ARRAY_DATA_TYPE = "visualization_msgs/MarkerArray"
    
    
    __CATEGORY_COLORS = dict()
    __NS_MARKER_ID = dict()
    __DATA_LOAD_FNS = {
            PCL_DATA_TYPE: np.load, 
            IMAGE_DATA_TYPE: cv2.imread, 
            MARKER_ARRAY_CUBE_DATA_TYPE: util.load_json_from_file
        }
    __ROS_MSG_FNS = {}

    @classmethod
    def create_cv_brigde(cls):
        cls.img_cv_bridge = cv_bridge.CvBridge()

    @classmethod
    def __category_color(cls, category=None):
        color = cls.__CATEGORY_COLORS.get(category, None)
        if color is None:
            color = list(np.random.choice(range(255),size=3))
            cls.__CATEGORY_COLORS[category] = color

        return color

    @classmethod
    def __marker_id(cls, ns=None):
        _id = cls.__NS_MARKER_ID.get(ns, None)

        if _id is not None:
            _id += 1
        else:
            _id = 0

        cls.__NS_MARKER_ID[ns] = _id

        return _id

    @classmethod
    def get_topics_types(cls, reader):
        topics = reader.get_type_and_topic_info()[1] 
        topic_types = dict() 

        for topic, topic_tuple in topics.items():
            topic_types[topic] = topic_tuple[0]
        
        return topic_types

    @classmethod
    def get_data_class(cls, data_type):
        data_class = None
        if data_type == cls.IMAGE_DATA_TYPE:
            data_class = Image
        elif data_type == cls.PCL_DATA_TYPE:
            data_class = PointCloud2
        elif data_type == cls.BUS_DATA_TYPE:
            data_class = Bus
        elif data_type == cls.MARKER_ARRAY_CUBE_DATA_TYPE:
            data_class = MarkerArray
        elif data_type == cls.MARKER_ARRAY_DATA_TYPE:
             data_class = MarkerArray
        else:
            raise ValueError("Data type not supported:" + str(data_type))

        return data_class

    @classmethod
    def __is_marker_array(cls, ros_msg):
        return "MarkerArray" in str(type(ros_msg))

    @classmethod
    def get_ros_msg_ts_nsecs(cls, ros_msg):
        return ros_msg.header.stamp.secs * 1000000 + int(ros_msg.header.stamp.nsecs/1000)
        
    @classmethod
    def set_ros_msg_received_time(cls, ros_msg):
        _ts = time.time()*1000000
        _stamp = divmod(_ts, 1000000 ) #stamp in micro secs
        
        if cls.__is_marker_array(ros_msg):
            markers = ros_msg.markers
            for marker in markers:
                marker.header.stamp.secs = int(_stamp[0]) # secs
                marker.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs
        else:
            ros_msg.header.stamp.secs = int(_stamp[0]) # secs
            ros_msg.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs


    @classmethod
    def set_ros_msg_header(cls, ros_msg=None, ts=None, frame_id=None):
        _stamp = divmod(ts, 1000000 ) #stamp in micro secs

        if cls.__is_marker_array(ros_msg):
            markers = ros_msg.markers
            for marker in markers:
                marker.header.frame_id = frame_id
                marker.header.stamp.secs = int(_stamp[0]) # secs
                marker.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs
        else:
            ros_msg.header.frame_id = frame_id
            ros_msg.header.stamp.secs = int(_stamp[0]) # secs
            ros_msg.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs

    @classmethod
    def bus_msg(cls, row=None):
        msg = Bus()

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

    @classmethod
    def __point_field(cls, name, offset, datatype=PointField.FLOAT32, count=1):
        return PointField(name, offset, datatype, count)

    @classmethod
    def get_pcl_fields(cls):
        return [
            cls.__point_field('x', 0),
            cls.__point_field('y', 4),
            cls.__point_field('z', 8),
            cls.__point_field('r', 12),
            cls.__point_field('g', 16),
            cls.__point_field('b', 20)
        ]

    @classmethod
    def pcl_sparse_msg(cls, points=None, reflectance=None, rows=None, cols=None, transform=None):
  
        if transform is not None:
            points_trans = cls.transform_points_frame(points=points, transform=transform)
            points = points_trans[:,0:3]

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
        
        msg.width = width
        msg.height = height
        
        msg.fields = cls.get_pcl_fields()

        msg.is_bigendian = False
        msg.point_step = 24
        msg.row_step = msg.point_step * width
        msg.is_dense = False
        data_array = np.array(np.hstack([pca, ca]), dtype=np.float32)
        msg.data = data_array.tostring()

        return msg

    @classmethod
    def pcl_dense_msg(cls, points=None, reflectance=None, transform=None):
  
        if transform is not None:
            points_trans = cls.transform_points_frame(points=points, transform=transform)
            points = points_trans[:,0:3]

        colors = np.stack([reflectance, reflectance, reflectance], axis=1)
        assert(points.shape == colors.shape)
    
        msg = PointCloud2()
        
        msg.width = points.shape[0]
        msg.height = 1
        
        msg.fields = cls.get_pcl_fields()

        msg.is_bigendian = False
        msg.point_step = 24
        msg.row_step = msg.point_step * msg.width
        msg.is_dense = True
        data_array = np.array(np.hstack([points, colors]), dtype=np.float32)
        msg.data = data_array.tostring()

        return msg

    @classmethod
    def __make_color(cls, rgb, a=1):
        c = ColorRGBA()
        c.r = rgb[0]
        c.g = rgb[1]
        c.b = rgb[2]
        c.a = a
        
        return c
    @classmethod
    def marker_cube_msg(cls, boxes=None, ns=None, lifetime=None, transform=None):
        marker_array_msg = MarkerArray()
        keys = boxes.keys()
        for key in keys:
            box =  boxes[key]

            marker = Marker()
            marker.ns = ns
            marker.id = cls.__marker_id(ns)
            category = box.get('class', "")
            marker.text = category
            marker.type = Marker.CUBE

            center = box.get('center', None)
            axis = box.get('axis', None)

            if transform is not None:
                points = np.expand_dims(center, axis=0)
                points_trans = cls.transform_points_frame(points=points, transform=transform)
                center = points_trans[0,0:3]

                points = np.expand_dims(axis, axis=0)
                points_trans = cls.transform_points_frame(points=points, transform=transform)
                axis = points_trans[0,0:3]

            rot_angle = box.get('rot_angle', None)
            size = box.get('size', None)
            quaternion = Quaternion(axis=axis, radians=rot_angle)

            marker.pose = cls.get_pose(position=center, orientation=quaternion)
            marker.frame_locked = True
            marker.scale.x = size[0]
            marker.scale.y = size[1]
            marker.scale.z = size[2]
            marker.color = cls.__make_color(cls.__category_color(category), 0.5)
            if lifetime is not None:
                marker.lifetime = lifetime
            marker_array_msg.markers.append(marker)

        return marker_array_msg

    @classmethod
    def get_pose(cls, position=None, orientation=None):

        p = Pose()
        p.position.x = position[0]
        p.position.y = position[1]
        p.position.z = position[2]
        
        p.orientation.w = orientation[0]
        p.orientation.x = orientation[1]
        p.orientation.y = orientation[2]
        p.orientation.z = orientation[3]
            
        return p

    @classmethod
    def undistort_image(cls, cvim, lens=None, dist_parms=None, intr_mat_dist=None, intr_mat_undist=None):
            
        if (lens == 'Fisheye'):
            return cv2.fisheye.undistortImage(cvim, intr_mat_dist,
                                        D=dist_parms, Knew=intr_mat_undist)
        elif (lens == 'Telecam'):
            return cv2.undistort(cvim, intr_mat_dist, 
                        distCoeffs=dist_parms, newCameraMatrix=intr_mat_undist)
        else:
            return cvim

    @classmethod
    def transform_points_frame(cls, points=None, transform=None):
        points_hom = np.ones((points.shape[0], 4))
        points_hom[:, 0:3] = points
        points_trans = (np.matmul(transform, points_hom.T)).T 
    
        return points_trans

    @classmethod
    def image_msg(cls, cvim=None, transform=None):
        if transform is not None:
            cvim = transform(cvim)
        
        return cls.img_cv_bridge.cv2_to_imgmsg(cvim)

    @classmethod
    def __load_data_from_file(cls, data_store=None, id=None, path=None, data=None, load_fn=None):
        ''' load data from file'''

        fs = data_store['input']
        config = data_store[fs]
        root = config['root']
        path = os.path.join(root, path)
        data[id] = load_fn(path)

    @classmethod
    def load_data_from_fs(cls, data_type=None, data_store=None, 
        data_files=None, data_loader=None, data=None, ts=None):

        data_loader.clear() 
        data.clear()
        ts.clear()

        idx = 0

        load_fn = cls.__DATA_LOAD_FNS[data_type]
        for f in data_files:
            path = f[1]
            data_loader[idx] = threading.Thread(target=cls.__load_data_from_file, 
                            kwargs={"data_store": data_store, 
                            "id": idx, "path": path, "data": data, "load_fn": load_fn})
        
            data_loader[idx].start()
            ts[idx]= int(f[2])
            idx += 1

        return idx

    @classmethod
    def get_camera_info(cls, cal_json=None, sensor=None):
        cam_name = sensor.rsplit("/", 1)[1]
        # get parameters from calibration json 
       
        intr_mat_undist = np.asarray(cal_json['cameras'][cam_name]['CamMatrix'])
        intr_mat_dist = np.asarray(cal_json['cameras'][cam_name]['CamMatrixOriginal'])
        dist_parms = np.asarray(cal_json['cameras'][cam_name]['Distortion'])
        lens = cal_json['cameras'][cam_name]['Lens']

        return lens, dist_parms, intr_mat_dist, intr_mat_undist

    @classmethod
    def get_undistort_fn(cls, cal_json=None, sensor=None):
        lens, dist_parms, intr_mat_dist, intr_mat_undist = cls.get_camera_info(cal_json=cal_json, sensor=sensor)

        def undistort_fn(cvim):
            return RosUtil.undistort_image(cvim, lens=lens, dist_parms=dist_parms, 
                    intr_mat_dist=intr_mat_dist, intr_mat_undist=intr_mat_undist) 
        
        return undistort_fn

    @classmethod
    def sensor_to_vehicle(cls, cal_json=None, sensor=None):
        cam_name = sensor.rsplit("/", 1)[1]
        return transform_from_to(cal_json['cameras'][cam_name]['view'], cal_json['vehicle']['view'])

    @classmethod 
    def __get_marker_lifetime(cls, request):
        try:
            marker_lifetime = request.get("marker_lifetime", None)
            if marker_lifetime is not None:
                return rospy.Duration.from_sec(marker_lifetime)
        except Exception:
            pass

        return None

    @classmethod
    def get_ros_msg_fn_params(cls, data_type=None, data=None, sensor=None, request=None, transform=None):
        params = None

        if data_type == cls.IMAGE_DATA_TYPE:
            params = {"cvim": data, 'transform': transform}
        elif data_type == cls.PCL_DATA_TYPE:
            params = dict()
            for key in data.keys():
                if "reflectance" in key:
                    params["reflectance"] =  data[key]
                elif "points" in key:
                    params["points"] = data[key]

                if len(params) == 2:
                    break
        elif data_type == cls.MARKER_ARRAY_CUBE_DATA_TYPE:
            lifetime = cls.__get_marker_lifetime(request)
            params =  {"boxes": data, "ns": sensor, "lifetime": lifetime}
        else:
            raise ValueError("Unsupported data type: {}".format(data_type))

        params['transform'] = transform
        return params

    @classmethod 
    def get_ros_msg_fn(cls, data_type):
        if len(cls.__ROS_MSG_FNS) == 0:
            cls.__ROS_MSG_FNS[cls.IMAGE_DATA_TYPE] = cls.image_msg
            cls.__ROS_MSG_FNS[cls.PCL_DATA_TYPE] = cls.pcl_dense_msg
            cls.__ROS_MSG_FNS[cls.MARKER_ARRAY_CUBE_DATA_TYPE] = cls.marker_cube_msg
            cls.__ROS_MSG_FNS[cls.BUS_DATA_TYPE] = cls.bus_msg
            
        return cls.__ROS_MSG_FNS.get(data_type, None)
       
    @classmethod 
    def get_data_load_fn(cls, data_type):
        return cls.__DATA_LOAD_FNS.get(data_type, None)

    @classmethod
    def drain_ros_msgs(cls, ros_msg_list=None, drain_ts=None):
        ros_msgs = []

        while ros_msg_list:
            next_ros_msg = ros_msg_list[0]
            next_ros_msg_ts_ns = RosUtil.get_ros_msg_ts_nsecs(next_ros_msg)

            if next_ros_msg_ts_ns <= drain_ts:
                msg = ros_msg_list.pop(0)
                ros_msgs.append(msg)
            else:
                break

        return ros_msgs