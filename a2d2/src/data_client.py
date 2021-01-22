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
from data_request import DataRequest
import json

class DataClient():
    def __init__(self, config):
        self.config = config
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("data_client")

        self.validate_requests()
        
    def request_data(self):

        try:
            tasks = []

            requests = self.config["requests"]
            for request in requests:
                t = DataRequest(servers=self.config["servers"], request=request)
                tasks.append(t)
                t.start()

            for t in tasks:
                t.join()

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def validate_requests(self):
        try:
            self.logger.info("validating data requests")
            requests = self.config["requests"]
            for request in requests:
                assert request["kafka_topic"]
                assert request["vehicle_id"]
                assert request["scene_id"]
                assert request["sensor_id"]
                assert request["accept"]

                assert int(request["start_ts"]) > 0 
                assert int(request["stop_ts"]) > int(request["start_ts"]) 
                assert int(request["step"]) > 0

                accept = request["accept"]
                if accept.endswith("rosbag") or accept.endswith("rosmsg"):
                    ros_topics = request["ros_topic"]
                    data_types = request["data_type"]
                    sensors = request["sensor_id"]
                    for s in sensors:
                        assert data_types[s]
                        assert ros_topics[s]

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))
            raise


def main(config):
    
    try:
        delay = int(config["delay"])
        time.sleep(delay)
    except:
        pass

    client = DataClient(config)
    client.request_data()
        
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka ds_consumer process')
    parser.add_argument('--config', type=str,  help='configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config)
