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
import json

from common.util import  validate_data_request, random_string
from common.ros_util import ROS_VERSION
from ros_data_node import RosDataNode

from std_msgs.msg import String
import subprocess

if ROS_VERSION == "1":
    import rospy
elif ROS_VERSION == "2":
    import rclpy
    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
else:
    raise ValueError("Unsupported ROS_VERSION:" + str(ROS_VERSION))

class RosBridgeService(RosDataNode):
    DATA_REQUEST_TOPIC = "/mozart/data_request"
    DATA_RESPONSE_TOPIC = "/mozart/data_response"
    DATA_REQUEST_CONTROL_TOPIC = "/mozart/data_request/control"

    def __init__(self, config=None):
        super().__init__(config=config)
        self.__logger = logging.getLogger("rosbridge_service")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self.__logger.info(f"Initialization complete {config}")

    def spin(self):

        node_name = f"mozart_datanode_{random_string(6)}"

        if ROS_VERSION == "1":
            subprocess.Popen(["roslaunch", "rosbridge_server", "rosbridge_websocket.launch"])

            self.__logger.info(f"Init ROS1 node: {node_name}, future log messages will be in ROS node log in container")
            rospy.init_node(node_name)

            rospy.Subscriber(self.DATA_REQUEST_TOPIC, String, self.__data_request_cb, queue_size=64)
            rospy.Subscriber(self.DATA_REQUEST_CONTROL_TOPIC, String, self.__data_request_control_cb, queue_size=64)
            self.__response_publisher = rospy.Publisher(self.DATA_RESPONSE_TOPIC, String, queue_size=64)
            rospy.spin()
        elif ROS_VERSION == "2":
            subprocess.Popen(["ros2", "launch", "rosbridge_server", "rosbridge_websocket_launch.xml"])

            rclpy.init()
            self.__logger.info(f"Create ROS2 node: {node_name}")
            self.__node = rclpy.create_node(node_name)

            self.__node.create_subscription(String,self.DATA_REQUEST_TOPIC,self.__data_request_cb, 64, callback_group=MutuallyExclusiveCallbackGroup())
            self.__node.create_subscription(String,self.DATA_REQUEST_CONTROL_TOPIC,self.__data_request_control_cb, 64, callback_group=MutuallyExclusiveCallbackGroup())
            self.__response_publisher = self.__node.create_publisher(String, self.DATA_RESPONSE_TOPIC, 64)
            executor = rclpy.executors.MultiThreadedExecutor(num_threads=2)
            executor.add_node(self.__node)
            executor.spin()


    def _ros2_node(self):
        return self.__node

    def _send_response_msg(self, json_msg):
        msg = String()
        msg.data = json.dumps(json_msg)
        self.__response_publisher.publish(msg)


    def __data_request_cb(self, ros_msg):
        try:  
            self.__logger.info(f"received ros message: {ros_msg.data}")
            
            request = json.loads(ros_msg.data)
            self.__logger.info(f"validate data request: {request}")
            validate_data_request(request, rosbridge=True)

            self.__logger.info(f"processing data request: {request}")
            self._init_request(request)

            if self._accept == "rosmsg":
                self._handle_rosmsg_request()
            elif self._accept.endswith("/rosbag"):
                self._handle_rosbag_request()
            elif self._accept == "manifest":
                self._handle_manifest_request()
            else:
                json_msg = {
                    "error": f"{self._accept} accept is not supported",
                    "__close__": True
                }
                self._send_response_msg(json_msg=json_msg)

        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def __data_request_control_cb(self, ros_msg):
        try:  
            self.__logger.info(f"received ros message: {ros_msg.data}")
            
            request_control = json.loads(ros_msg.data)
            command = request_control.get("command", None)

            if command == self.MAX_RATE:
                self._set_max_rate(request_control.get(self.MAX_RATE, 0))
            elif command == self.PAUSE or command == self.PLAY or command == self.STOP:
                self._set_request_state(command)
          
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ros Data Node')
    parser.add_argument('--config', type=str,  help='configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    ros_data_node = RosBridgeService(config)
    ros_data_node.spin()