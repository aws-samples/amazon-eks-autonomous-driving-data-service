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
import json
import logging
import cv2
import os

from tempfile import NamedTemporaryFile
from common.view import transform_from_to
from common.util import  download_s3_file,  get_s3_client
from a2d2_msgs.msg import Bus

from common.ros_util import RosUtil

class A2d2RosUtil(RosUtil):
    BUS_DATA_TYPE = 'a2d2_msgs/Bus'

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
        self.__logger = logging.getLogger("A2d2RosUtil")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        
        if bucket and key:
            self.__cal_json = self.__get_calibration_json(bucket=bucket, key=key)

    def __get_calibration_json(self, bucket: str, key:str) -> dict:
        s3_client =  get_s3_client()
        with NamedTemporaryFile(mode='w+', delete=False) as cal_file:
            download_s3_file(s3_client=s3_client, bucket=bucket, 
                            key=key, local_path=cal_file.name, logger=self.__logger)
            cal_file.close()
        
            with open(cal_file.name, "r") as json_file:
                cal_json = json.load(json_file)
                self.__logger.info(cal_json)
                os.remove(json_file.name)
                return cal_json
        
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

    def sensor_to_vehicle(self, sensor:str) -> Any:    
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

        cam_name = sensor.rsplit("/", 1)[1]
        return transform_from_to(self.__cal_json['cameras'][cam_name]['view'], self.__cal_json['vehicle']['view'])

    def vehicle_to_sensor(self, sensor:str) -> Any:
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

        cam_name = sensor.rsplit("/", 1)[1]
        return transform_from_to(self.__cal_json['vehicle']['view'], self.__cal_json['cameras'][cam_name]['view'])

    def get_undistort_fn(self, sensor: str) -> Callable:
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

        lens, dist_parms, intr_mat_dist, intr_mat_undist = self.__get_camera_info(sensor=sensor)

        def undistort_fn(cvim):
            return self.undistort_image(cvim, lens=lens, dist_parms=dist_parms, 
                    intr_mat_dist=intr_mat_dist, intr_mat_undist=intr_mat_undist) 
        
        return undistort_fn
    
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