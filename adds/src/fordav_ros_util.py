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

from typing import Any, Callable, Sequence, Union
import logging

from common.util import  download_s3_file,  get_s3_client
from fordav_msgs.msg import Bus

import yaml
import numpy as np
import numpy.linalg as linalg
from scipy.spatial.transform import Rotation as R
import re
import tempfile
import os

from common.ros_util import RosUtil

class DatasetRosUtil(RosUtil):
    BUS_DATA_TYPE = 'fordav_msgs/Bus'
    VEHICLES = ["V1", "V2", "V3"]
    SENSOR_NAME_ID = {
        "cameraCenter": "camera_center",
        "cameraFrontRight": "camera_front_right",
        "cameraFrontLeft": "camera_front_left",
        "cameraRearLeft": "camera_rear_left",
        "cameraRearRight": "camera_rear_right",
        "cameraSideLeft": "camera_side_left",
        "cameraSideRight": "camera_side_right",
        "lidarRed": "lidar_red_pointcloud",
        "lidarYellow": "lidar_yellow_pointcloud",
        "lidarBlue": "lidar_blue_pointcloud",
        "lidarGreen": "lidar_green_pointcloud",
        "imu": "imu"
    }

    def __init__(self, bucket: str = None, key: str = None):
        """Constructor

        Parameters
        ----------
        bucket: str
            S3 bucket for A2D2 calibration JSON file

        key: str
            S3 bucket key for A2D2 calibration JSON file

        """

        super().__init__(bucket=bucket, key=key)
        self.__logger = logging.getLogger("DatasetRosUtil")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        
        if bucket and key:
            self.__s3_client = get_s3_client()
            self.__get_calibration_data(bucket=bucket, key=key)


    def __s3_bucket_keys(self, bucket_name:str, bucket_prefix:str):
        """Generator for listing S3 bucket keys matching prefix"""

        kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
        while True:
            resp = self.__s3_client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                yield obj['Key']

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def __get_transform_matrix(self, bucket:str, key:str) -> Any:
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".yaml", delete=False) as yaml_file:

            download_s3_file(s3_client=self.__s3_client,
                              bucket=bucket, key=key, 
                              local_path=yaml_file.name,
                              logger=self.__logger)
            
            yaml_file.close()
            transform_matrix = None
            try:
                with open(yaml_file.name, "r") as stream:
                    cal_data = yaml.safe_load(stream)
                    transform = cal_data["transform"]
                    rot = transform["rotation"]
                    translation = transform["translation"]
                    trans_vec = np.array([translation['x'], translation['y'], translation['z']]).T

                    rot_qt = np.array([rot['x'], rot['y'], rot['z'], rot['w']])
                    r = R.from_quat(rot_qt)
                    rot_matrix = r.as_matrix()
                    transform_matrix = np.eye(4)
                    transform_matrix[0:3,0:3] = rot_matrix
                    transform_matrix[0:3, -1] = trans_vec
                    self.__logger.info(f"Transform matrix {key}: {transform_matrix}")
            except Exception as exc:
                self.__logger.error(exc)
            finally:
                os.remove(yaml_file.name)

            return transform_matrix
        
    def __get_calibration_data(self, bucket: str, key:str) -> None:
        self.__cal_data = dict()

        for v in self.VEHICLES:
            self.__cal_data[v] = dict()

        for key in self.__s3_bucket_keys(bucket_name=bucket, bucket_prefix=key):
            m = re.match(".+\/Calibration\/(\w+)\/(\w+)_body\.yaml", key)
            if m:
                vehicle_id = m[1]
                sensor_name = m[2]
                sensor_id = self.SENSOR_NAME_ID.get(sensor_name, sensor_name)

                transform_matrix = self.__get_transform_matrix(bucket=bucket, key=key)
                self.__cal_data[vehicle_id][sensor_id] = transform_matrix

    def get_bus_datatype(self) -> str:
        """Return Ros message datatype for bus data

        Returns
        -------
        str
            Ros message datatype for bus data
        """

        return self.BUS_DATA_TYPE
   
    def get_bus_dataclass(self) -> type:
        """Get Ros message data class for bus data

        Returns
        -------
        type
            Ros message data class for bus data type
        """

        return Bus
    
    def bus_msg(self, row: Sequence[Union[str, int, float]]) -> Bus:
        """Return Ros message for a given row of bus data

        Parameters
        ----------
        row: Sequence[Union[str, int, float]
            A row of bus data

        Returns
        -------
        Bus
            Ros message for a row of bus data
        
        """

        msg = Bus()

        # Angular velocity
        msg.angular_velocity.x = float(row[3])
        msg.angular_velocity.y = float(row[4])
        msg.angular_velocity.z = float(row[5])
       
        # linear acceleration
        msg.linear_acceleration.x = float(row[6])
        msg.linear_acceleration.y = float(row[7])
        msg.linear_acceleration.z = float(row[8])
        
        msg.latitude = float(row[9])
        msg.longitude = float(row[10])
        msg.altitude = float(row[11])
        
        return msg

    def sensor_to_vehicle(self, sensor:str, vehicle:str=None) -> Any:  
        """Return transform matrix from sensor ro vehicle frame

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Any
            Numpy array transform matrix shape [4, 4]
        
        """
        transform_matrix = None
        try:
            transform_matrix = self.__cal_data[vehicle][sensor]
        except Exception as e:
            self.__logger.error(e)
        
        return transform_matrix

    def vehicle_to_sensor(self, sensor:str, vehicle:str=None) -> Any: 
        """Return transform matrix from vehicle ro sensor frame

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Any
            Numpy array transform matrix shape [4, 4]
        
        """
        transform_matrix = None
        try:
            transform_matrix = self.__cal_data[vehicle][sensor]
            transform_matrix = linalg.inv(transform_matrix)
        except Exception as e:
            self.__logger.error(e)
        
        return transform_matrix

    def get_undistort_fn(self, sensor: str, vehicle:str=None) -> Callable:
        """Return a function that undistorts sensor data

        Parameters
        ----------
        sensor: str
            Sensor id

        Returns
        -------
        Callable
            Callable function for undistorting sensor data, e.g. Open CV image data
        
        """   
        return (lambda cvim: cvim)
    