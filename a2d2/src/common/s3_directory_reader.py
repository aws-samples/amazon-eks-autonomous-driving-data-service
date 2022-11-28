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
from multiprocessing import Process, Queue
import logging
import os, shutil
import signal

from common.util import  get_s3_client, random_string, download_s3_directory, mkdir_p

class S3DirectoryReader(Process):
    def __init__(self, tmp_dir=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.__logger = logging.getLogger(f"s3_directory_reader")

        self.__req = Queue()
        self.__resp = Queue()
        self.__s3_client = get_s3_client()
        self.__tmp_dir = os.path.join(tmp_dir, random_string())
        mkdir_p(self.__tmp_dir)

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)

        self.__logger.info("process initialized")

    def request_queue(self):
        return self.__req

    def response_queue(self):
        return self.__resp

    def __exit_gracefully(self, signum, frame):
        self.__logger.info("Received {} signal".format(signum))
        self.__close_reader()

    def __close_reader(self):
        self.__logger.info(f"Cleaning tmp dir: {self.__tmp_dir}")
        shutil.rmtree(self.__tmp_dir, ignore_errors=True)
        self.__logger.info("process exiting")
        sys.exit()

    def run(self):
        self.__logger.info("process running")
        while True:
            try:
                msg = self.__req.get(block=True)
                if msg == "__close__":
                    self.__close_reader()

                s3_info = msg.split(" ", 1)
                bucket = s3_info[0]
                bucket_prefix = s3_info[1]
                local_path = os.path.join(self.__tmp_dir, random_string())
                download_s3_directory(s3_client=self.__s3_client, 
                    bucket=bucket, 
                    bucket_prefix=bucket_prefix, 
                    local_path=local_path,
                    logger=self.__logger)
                self.__resp.put(local_path + " " + bucket + " " + bucket_prefix)
            except Exception as _:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                self.__logger.error(str(exc_type))
                self.__logger.error(str(exc_value))
    

