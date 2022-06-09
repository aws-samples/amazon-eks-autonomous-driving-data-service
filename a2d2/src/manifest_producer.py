
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
from multiprocessing import Process
import logging
import json
import threading
import signal

from kafka import KafkaProducer
from manifest_dataset import ManifestDataset
from bus_dataset import BusDataset
from util import random_string, is_cancel_msg, send_kafka_msg

class ManifestProducer(Process):
    def __init__(self, dbconfig=None, servers=None, request=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("manifest_producer")

        self.servers = servers
        self.request = request

        self.manifests = [] 
        sensors = self.request['sensor_id']
        for s in sensors:
            self.manifests.append(self.create_manifest(dbconfig=dbconfig, sensor_id=s))

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)


    def create_manifest(self, dbconfig=None, sensor_id=None):
        if sensor_id == 'bus':
            manifest = BusDataset(dbconfig=dbconfig, 
                        vehicle_id=self.request["vehicle_id"],
                        scene_id=self.request["scene_id"],
                        start_ts=int(self.request["start_ts"]), 
                        stop_ts=int(self.request["stop_ts"]),
                        step=int(self.request["step"]))
        else:
            manifest = ManifestDataset(dbconfig=dbconfig, 
                        vehicle_id=self.request["vehicle_id"],
                        scene_id=self.request["scene_id"],
                        sensor_id=sensor_id,
                        start_ts=int(self.request["start_ts"]), 
                        stop_ts=int(self.request["stop_ts"]),
                        step=int(self.request["step"]))

        return manifest

    def publish_manifest(self, manifest=None):

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.servers, 
                    client_id=random_string())

            response_topic = self.request["response_topic"]
            while True:
                content = manifest.fetch()
                if not content:
                    break

                json_msg = {"type": "manifest", "content": content}  
                self.producer.send(response_topic, json.dumps(json_msg).encode('utf-8'))
                self.producer.flush()

                if self.request.get('preview', False):
                    break

            json_msg = {"__close__": True}  
            self.producer.send(response_topic, json.dumps(json_msg).encode('utf-8'))
            self.producer.flush()
            self.producer.close()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def __close(self):
        try:
            resp_topic = self.request['response_topic']
            json_msg = {"__close__": True} 
            self.producer.send(resp_topic, json.dumps(json_msg).encode('utf-8'))

            self.producer.flush()
            self.producer.close()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))
        finally:
            self.logger.info("Completed request:"+resp_topic)
            sys.exit(0)

    def run(self):
        try:
            tasks = []
            for m in self.manifests:
                t = threading.Thread(target=self.publish_manifest, kwargs={"manifest": m})
                t.start()
                tasks.append(t)

            for t in tasks:
                t.join()

            sys.exit(0)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.logger.error(str(exc_type))
            self.logger.error(str(exc_value))

    def __exit_gracefully(self, signum, frame):
        self.logger.info("Received {} signal".format(signum))
        self.__close()