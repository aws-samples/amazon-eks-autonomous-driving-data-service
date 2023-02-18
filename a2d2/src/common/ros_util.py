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

import time
from typing import Any, Callable, Sequence, Union
import numpy as np
import cv2
import threading
import os

from  common.util import load_json_from_file

import cv_bridge
from sensor_msgs.msg import Image, PointCloud2, PointField
from visualization_msgs.msg import  Marker, MarkerArray
from geometry_msgs.msg import  Pose
from  pyquaternion import Quaternion
from std_msgs.msg import ColorRGBA

ROS_VERSION = os.getenv("ROS_VERSION", "1")

if ROS_VERSION == "1":
    import rospy
elif ROS_VERSION == "2":
    import rclpy
else:
    raise ValueError("Unsupported ROS_VERSION:" + str(ROS_VERSION))

class RosUtil(ABC):
    PCL_DATA_TYPE = "sensor_msgs/PointCloud2"
    IMAGE_DATA_TYPE = "sensor_msgs/Image"
    MARKER_ARRAY_CUBE_DATA_TYPE = "visualization_msgs/MarkerArray/Marker/CUBE"
    MARKER_ARRAY_DATA_TYPE = "visualization_msgs/MarkerArray"
    
    __CATEGORY_COLORS = dict[str, list[int, int, int]]()
    __NS_MARKER_ID = dict[str, int]()
    __DATA_LOAD_FNS = {
            PCL_DATA_TYPE: np.load, 
            IMAGE_DATA_TYPE: cv2.imread, 
            MARKER_ARRAY_CUBE_DATA_TYPE: load_json_from_file
    }

    __ROS_MSG_FNS = dict()

    @staticmethod
    def dynamic_import(name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def __init__(self, bucket: str = None, key: str = None):
        """Constructor

        Parameters
        ----------
        bucket: str
            S3 bucket for calibration data

        key: str
            S3 bucket key for calibration data

        """

        self._cal_bucket = bucket
        self._cal_key = key
        
    
    @abstractmethod
    def get_bus_datatype(self) -> str:
        """Get Ros message datatype for bus data

        Returns
        -------
        str
            Ros message datatype for bus data
        """

        return NotImplemented
    
    @abstractmethod
    def get_bus_dataclass(self) -> type:
        """Get Ros message data class for bus data

        Returns
        -------
        type
            Ros message data class for bus data type
        """

        return NotImplemented

    def bus_msg(self, row: Sequence[Union[str, int, float]]) -> Any:
        """Get Ros message for a row of bus data

        Parameters
        ----------
        row: Sequence[Union[str, int, float]]
            One row of Ros bus data for a single timestamp

        Returns
        -------
        Any
            Ros message for bus data
        """

        return NotImplemented

    @abstractmethod
    def sensor_to_vehicle(self, sensor:str) -> Any:    
        """Get sensor to vehicle transform matrix 

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Any
            Numpy array transform matrix of shape [4,4]
        """
         
        return NotImplemented

    @abstractmethod
    def vehicle_to_sensor(self, sensor:str) -> Any:
        """Get vehicle to sensor transform matrix 

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Any
            Numpy array transform matrix of shape [4,4]
        """

        return NotImplemented

    @abstractmethod
    def get_undistort_fn(self, sensor: str) -> Callable:
        """Get function for undistorting  single frame of sensor data. 
        This method is typically used to get the function for 
        undistorting single Open CV image obtained from a given sensor. 

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Any
            Function for undistorting single frame of sensor data
        """

        return NotImplemented
        
    @classmethod
    def create_cv_brigde(cls):
        """Create CV bridge"""
        cls.img_cv_bridge = cv_bridge.CvBridge()

    def get_data_class(self, data_type: str):
        data_class = None
        if data_type == self.IMAGE_DATA_TYPE:
            data_class = Image
        elif data_type == self.PCL_DATA_TYPE:
            data_class = PointCloud2
        elif data_type == self.get_bus_datatype():
            data_class = self.get_bus_dataclass()
        elif data_type == self.MARKER_ARRAY_CUBE_DATA_TYPE:
            data_class = MarkerArray
        elif data_type == self.MARKER_ARRAY_DATA_TYPE:
             data_class = MarkerArray
        else:
            raise ValueError("Data type not supported:" + str(data_type))

        return data_class
    
    @classmethod
    def __category_color(cls, category:str) -> list[int, int, int]:
        color = cls.__CATEGORY_COLORS.get(category, None)
        if color is None:
            color = list(np.random.choice(range(255),size=3))
            cls.__CATEGORY_COLORS[category] = color

        return color

    @classmethod
    def __marker_id(cls, ns: str) -> int:
        _id = cls.__NS_MARKER_ID.get(ns, None)

        if _id is not None:
            _id += 1
        else:
            _id = 0

        cls.__NS_MARKER_ID[ns] = _id

        return _id

    @classmethod
    def get_topics_types(cls, reader: Any) -> dict:
        """Get Ros topic types for a given serialized message reader,
        e.g. Ros bag reader.

        Parameters
        ----------
        reader: Any
            Ros message reader

        Returns
        -------
        dict
            Ros topic types
        """
         
        topic_types = dict() 

        if ROS_VERSION == "1":
            topics = reader.get_type_and_topic_info()[1] 
            
            for topic, topic_tuple in topics.items():
                topic_types[topic] = topic_tuple[0]
        
        elif ROS_VERSION == "2":
            topics_and_types = reader.get_all_topics_and_types()
            topic_types = {topics_and_types[i].name: topics_and_types[i].type for i in range(len(topics_and_types))}

        return topic_types


    @classmethod
    def __is_marker_array(cls, ros_msg: Any) -> bool:
        return "MarkerArray" in str(type(ros_msg))

    @classmethod
    def get_ros_msg_ts_nsecs(cls, ros_msg: Any) -> float:
        """Get Ros message header timestamp in nanoseconds

        Parameters
        ----------
        ros_msg: Any
            Ros message

        Returns
        -------
        float
            Ros message time stamp in header in nanoseconds
        """

        if ROS_VERSION == "1":
            return ros_msg.header.stamp.secs * 1000000 + int(ros_msg.header.stamp.nsecs/1000)
        elif ROS_VERSION == "2":
            return ros_msg.header.stamp.sec * 1000000 + int(ros_msg.header.stamp.nanosec/1000)
        
    @classmethod
    def set_ros_msg_received_time(cls, ros_msg: Any):
        """Set current time in Ros message header

        Parameters
        ----------
        ros_msg: Any
            Ros message
    
        """

        _ts = time.time()*1000000
        _stamp = divmod(_ts, 1000000 ) #stamp in micro secs
      
        if ROS_VERSION == "1":
            if cls.__is_marker_array(ros_msg):
                markers = ros_msg.markers
                for marker in markers:
                    marker.header.stamp.secs = int(_stamp[0]) # secs
                    marker.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs
            else:
                ros_msg.header.stamp.secs = int(_stamp[0]) # secs
                ros_msg.header.stamp.nsecs = int(_stamp[1]*1000) # nano secs
        elif ROS_VERSION == "2":
            if cls.__is_marker_array(ros_msg):
                markers = ros_msg.markers
                for marker in markers:
                    marker.header.stamp.sec = int(_stamp[0]) # secs
                    marker.header.stamp.nanosec = int(_stamp[1]*1000) # nano secs
            else:
                ros_msg.header.stamp.sec = int(_stamp[0]) # secs
                ros_msg.header.stamp.nanosec = int(_stamp[1]*1000) # nano secs

    @classmethod
    def set_ros_msg_header(cls, ros_msg: Any, ts: float, frame_id: str):
        """Set Ros message header

        Parameters
        ----------
        ros_msg: Any
            Ros message
        ts: float
            Message header time stamp
        frame_id: str
            Ros message frame id
    
        """

        _stamp = divmod(ts, 1000000 ) #stamp in micro secs

        if ROS_VERSION == "1":
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
        elif ROS_VERSION == "2":
            if cls.__is_marker_array(ros_msg):
                markers = ros_msg.markers
                for marker in markers:
                    marker.header.frame_id = frame_id
                    marker.header.stamp.sec = int(_stamp[0]) # secs
                    marker.header.stamp.nanosec = int(_stamp[1]*1000) # nano secs
            else:
                ros_msg.header.frame_id = frame_id
                ros_msg.header.stamp.sec = int(_stamp[0]) # secs
                ros_msg.header.stamp.nanosec = int(_stamp[1]*1000) # nano secs

    @classmethod
    def __point_field(cls, name:str, offset:int, datatype=PointField.FLOAT32, count=1):
        pf = None
        if ROS_VERSION == "1":
            pf = PointField(name, offset, datatype, count)
        elif ROS_VERSION == "2":
            pf = PointField()
            pf.name = name
            pf.offset = offset
            pf.datatype = datatype
            pf.count = count
        
        return pf

    @classmethod
    def __get_pcl_fields(cls):
        return [
            cls.__point_field('x', 0),
            cls.__point_field('y', 4),
            cls.__point_field('z', 8),
            cls.__point_field('r', 12),
            cls.__point_field('g', 16),
            cls.__point_field('b', 20)
        ]

    @classmethod
    def pcl_sparse_msg(cls, points:Any, reflectance:Any, rows:Any, cols:Any, transform:Any) -> PointCloud2:
        """Get Ros sparse point cloud message

        Parameters
        ----------
        points: Any
            Numpy array of points shape [N, 3]
        reflectance: Any
            Numpy array of reflectance values shape [N,]
        rows: Any
            Numpy array point cloud rows shape [N,]
        cols: Any
            Numpy array point cloud cols shape [N,]
        transform: Any
            Numpy array transform matrix shape [4, 4]

        Returns
        -------
        PointCloud2
            Ros sparse point cloud message
        """

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
        
        msg.fields = cls.__get_pcl_fields()

        msg.is_bigendian = False
        msg.point_step = 24
        msg.row_step = msg.point_step * width
        msg.is_dense = False
        data_array = np.array(np.hstack([pca, ca]), dtype=np.float32)
        msg.data = data_array.tostring()

        return msg

    @classmethod
    def pcl_dense_msg(cls, points:Any, reflectance:Any, transform:Any) -> PointCloud2:
        """Get Ros dense point cloud message

        Parameters
        ----------
        points: Any
            Numpy array of points shape [N, 3]
        reflectance: Any
            Numpy array of reflectance values shape [N,]
        transform: Any
            Numpy array transform matrix shape [4, 4]

        Returns
        -------
        PointCloud2
            Ros dense point cloud message
        """

        if transform is not None:
            points_trans = cls.transform_points_frame(points=points, transform=transform)
            points = points_trans[:,0:3]

        colors = np.stack([reflectance, reflectance, reflectance], axis=1)
        assert(points.shape == colors.shape)
    
        msg = PointCloud2()
        
        msg.width = points.shape[0]
        msg.height = 1
        
        msg.fields = cls.__get_pcl_fields()

        msg.is_bigendian = False
        msg.point_step = 24
        msg.row_step = msg.point_step * msg.width
        msg.is_dense = True
        data_array = np.array(np.hstack([points, colors]), dtype=np.float32)
        msg.data = data_array.tostring()

        return msg

    @classmethod
    def __make_color(cls, rgb:list[int, int, int], a=1) -> ColorRGBA:
        c = ColorRGBA()
        c.r = rgb[0]
        c.g = rgb[1]
        c.b = rgb[2]
        c.a = a
        
        return c
    
    @classmethod
    def marker_cube_msg(cls, boxes:Any, ns:Any, lifetime:Any, transform:Any) -> Any:
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
    def get_pose(cls, position:Any, orientation:Any) -> Pose:
        """Get Ros Pose for given position and orientation

        Parameters
        ----------
        position: Any
            Numpy array of position shape [3]
        orientation: Any
            Numpy array of orientation shape [4]
        
        Returns
        -------
        Pose
            Ros Pose
        """
         
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
    def transform_points_frame(cls, points: Any, transform: Any) -> Any:
        """Transform points' reference frame using transform matrix

        Parameters
        ----------
        points: Any
            Numpy array of points to transform shape [N, 3]
        transform: Any
            Numpy transform matrix shape [4, 4]
        
        Returns
        -------
        Any
            Transformed homegeneous points Numpy array shape [N, 4]
        """

        points_hom = np.ones((points.shape[0], 4))
        points_hom[:, 0:3] = points
        points_trans = (np.matmul(transform, points_hom.T)).T 
    
        return points_trans

    @classmethod
    def image_msg(cls, cvim:Any, transform:Any) -> Image:
        """Convert Open CV image to Ros Image message

        Parameters
        ----------
        cvim: Any
            Open CV image array
        transform: Any
            Transform matrix
        
        Returns
        -------
        Image
            Ros Image message
        """
        
        if transform is not None:
            cvim = transform(cvim)
        
        return cls.img_cv_bridge.cv2_to_imgmsg(cvim)

    @classmethod
    def __load_data_from_file(cls, data_store:dict, id:Any, path:str, data:dict, load_fn: Callable):
        ''' load data from file'''

        fs = data_store['input']
        config = data_store[fs]
        root = config['root']
        path = os.path.join(root, path)
        data[id] = load_fn(path)

    @classmethod
    def load_data_from_fs(cls, 
                          data_type:str , 
                          data_store:dict, 
                          data_files:Sequence, 
                          data_loader: dict, 
                          data: dict, 
                          ts: dict) -> int:
        """Load data from file-system using multiple threads

        Parameters
        ----------
        data_type: str
            Ros message data type
        data_store: dict
            Data store configuration
        data_files: Sequence
            Sequence of files to be loaded
        data_loader: dict
            This is used to return the data loader threads. This is cleared before use.
        data: dict
            This is used to return the loaded data. This is cleared before use.
        ts: dict
            This is used to return the loaded data timestamps. This is cleared before use.

        Returns
        -------
        int
            Number of data loader threads
        """

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
    def __get_marker_lifetime(cls, request: Any) -> Any:
        try:
            marker_lifetime = request.get("marker_lifetime", None)
            if marker_lifetime is not None:
                if ROS_VERSION == "1":
                    return rospy.Duration.from_sec(marker_lifetime)
                elif ROS_VERSION == "2":
                    return rclpy.time.Duration(seconds=marker_lifetime)
        except Exception:
            pass

        return None

    @classmethod 
    def get_data_load_fn(cls, data_type):
        return cls.__DATA_LOAD_FNS.get(data_type, None)
    
    @classmethod
    def get_ros_msg_fn(cls, data_type: str) -> Callable:
        """Get Ros message function for converting given datatype to a Ros message.
        See also get_ros_msg_fn_params. This is not applicable to bus data.
        See also bus_msg.

        Parameters
        ----------
        data_type: str
            Ros message data type

        Returns
        -------
        Callable
            Ros message function to be invoked with parameters returned by get_ros_msg_fn_params
        """
        
        if len(cls.__ROS_MSG_FNS) == 0:
            cls.__ROS_MSG_FNS[cls.IMAGE_DATA_TYPE] = cls.image_msg
            cls.__ROS_MSG_FNS[cls.PCL_DATA_TYPE] = cls.pcl_dense_msg
            cls.__ROS_MSG_FNS[cls.MARKER_ARRAY_CUBE_DATA_TYPE] = cls.marker_cube_msg

        return cls.__ROS_MSG_FNS.get(data_type, None)
   
    @classmethod
    def get_ros_msg_fn_params(cls, 
                              data_type: str, 
                              data: Any, 
                              sensor: str, 
                              request: Any, 
                              transform: Any) -> dict:
        """Get Ros message function parameters for a given datatype. 
        The returned parameters are used to invoke the Ros message function 
        returned by get_ros_msg_fn.

        Parameters
        ----------
        data_type: str
            Ros message data type
        data: Any
            Arbitrary data
        sensor: str
            Sensor id
        request: Any
            Data request object
        transform: Any
            Transform matrix

        Returns
        -------
        dict
            Ros message function parameters
        """

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
    def drain_ros_msgs(cls, ros_msg_list: list, drain_ts: float) -> list:
        """Drains input Ros message list and returns Ros messages upto drain timestamp

        Parameters
        ----------
        ros_msg_list: list
            Input list of Ros msgs of any type
        drain_ts: float
            Drain messages upto this timestamp from input list

        Returns
        -------
        list
            List of drained Ros messages
        """

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