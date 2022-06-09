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
from multiprocessing import Process, Lock, Value
import logging
import json
import signal


from kafka import KafkaProducer
from util import random_string
from rosbag_consumer import RosbagConsumer
from manifest_consumer import ManifestConsumer


class DataRequest(Process):
    def __init__(self, servers=None, request=None, use_time=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("data_request")

        self.servers = servers
        self.request = request
        self.use_time = use_time

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)
        self.__tasks = []
        
    def request_rosbag(self):
        try:
            producer = KafkaProducer(bootstrap_servers=self.servers, 
                    client_id=random_string())

            response_topic = random_string()
            s3 = self.request["accept"].startswith("s3/")
            t = RosbagConsumer(servers=self.servers, 
                response_topic=response_topic, 
                s3=s3, use_time=self.use_time, 
                no_playback=self.request.get("no_playback", False),
                no_delete = self.request.get("no_delete", False))
            self.__tasks.append(t)
            t.start()

            self.request["response_topic"] = response_topic

            msg = {"request": self.request}
            self.logger.info("send message: {0}".format(msg))
            producer.send(self.request["kafka_topic"], json.dumps(msg).encode('utf-8'))
            producer.flush()
            producer.close()

            t.join()
            self.__tasks.remove(t)
        except Exception as _:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def request_manifest(self):
        try:
            producer = KafkaProducer(bootstrap_servers=self.servers, 
                    client_id=random_string())

            response_topic = random_string()
            t = ManifestConsumer(servers=self.servers, response_topic=response_topic)
            self.__tasks.append(t)
            t.start()

            self.request["response_topic"] = response_topic
            msg = {"request": self.request}
            self.logger.info("send message: {0}".format(msg))
            producer.send(self.request["kafka_topic"], json.dumps(msg).encode('utf-8'))
            producer.flush()
            producer.close()

            t.join()
            self.__tasks.remove(t)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def run(self):

        accept = self.request['accept']
        if accept.endswith("rosbag"):
            self.request_rosbag()
        elif accept.endswith("manifest"):
            self.request_manifest()
        else:
            self.logger.error("Unexpected accept type: {0}".format(accept))
            raise ValueError()

    def __exit_gracefully(self, signum, frame):
        self.logger.error("Received {} signal".format(signum))

        for t in self.__tasks:
            t.terminate()
        self.__tasks.clear()
        sys.exit(0)


