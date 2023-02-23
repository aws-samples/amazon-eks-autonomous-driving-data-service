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
import time
from typing import Any

from common.util import  random_string, validate_data_request
from ros_data_node import RosDataNode
from kafka import KafkaConsumer, KafkaProducer

class DataService(RosDataNode):

    MAX_POLL_INTERVAL_MS = int(24*3600*1000)

    def __init__(self, config=None):
        super().__init__(config=config)
        self.__logger = logging.getLogger("data_service")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

       
        self.__logger.info(f"Initialization complete {config}")


    def __handle_message(self, message: Any):
        try: 
            json_msg = json.loads(message.value) 
            request = json_msg["request"]
            self.__logger.info("recvd request: {0}".format(request))
            validate_data_request(request)
            self.__logger.info("validated request successfully")
            
            self.__init_request(request)
            self.__logger.info("processing data request: {0}".format(request))

            accept = request["accept"]
            if accept.endswith("/rosbag"):
                self._handle_rosbag_request()
            elif accept == "manifest":
                self._handle_manifest_request()
            else:
                json_msg = {
                    "error": f"{accept} accept is not supported",
                    "__close__": True
                }
                self._send_response_msg(json_msg=json_msg)
        except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                self.__logger.error(str(exc_type))
                self.__logger.error(str(exc_value))

    def spin(self):

       
        topic = self._config["kafka_topic"]
        dataset = self._config["dataset"]
        schema_name = dataset["schema_name"]
        self.__consumer = KafkaConsumer(topic, 
                    bootstrap_servers=self._config["servers"],
                    client_id=random_string(),
                    max_poll_records=1,
                    max_poll_interval_ms=self.MAX_POLL_INTERVAL_MS,
                    auto_offset_reset="earliest",
                    group_id=f"{schema_name}-data-service")
        self.__producer = KafkaProducer(bootstrap_servers=self._config["servers"], client_id=random_string())
        self.__start_ts = time.time()

        for message in self.__consumer:
            try:
                self.__handle_message(message=message)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                self.__logger.error(str(exc_type))
                self.__logger.error(str(exc_value))

    def __init_request(self, request: dict):
        self.__response_topic = request["response_topic"]
        del request["response_topic"]
        del request["kafka_topic"]

        self._init_request(request)

    def _send_response_msg(self, json_msg: dict):
        json_msg['start_ts'] = self.__start_ts
        self.__producer.send(self.__response_topic, json.dumps(json_msg).encode('utf-8'))

    def _ros2_node(self):
        raise NotImplementedError("_ros2_node is not implemented")
        
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Data service')
    parser.add_argument('--config', type=str,  help='configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    data_service = DataService(config)
    data_service.spin()