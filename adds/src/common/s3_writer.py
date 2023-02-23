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
import shutil

from common.util import  get_s3_client, upload_s3 

class S3Writer(Process):
    def __init__(self, delete_local_path=False):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.__logger = logging.getLogger("s3_writer")

        self.__req = Queue()
        self.__resp = Queue()
        self.__delete_local = delete_local_path
        self.__s3_client = get_s3_client()

    def request_queue(self) -> Queue:
        return self.__req

    def response_queue(self) -> Queue:
        return self.__resp

    def run(self):
        while True:
            try:
                msg = self.__req.get(block=True)
                if msg == "__close__":
                    break

                s3_info = msg.split(" ")
                local_path = s3_info[0]
                bucket = s3_info[1]
                bucket_path = s3_info[2]
                
                upload_s3(s3_client=self.__s3_client, 
                    local_path=local_path, 
                    bucket=bucket,
                    bucket_path=bucket_path,
                    logger=self.__logger)

                if self.__delete_local:
                    shutil.rmtree(local_path, ignore_errors=True)

                self.__resp.put(f"{bucket}/{bucket_path}")
            except Exception as _:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                self.__logger.error(str(exc_type))
                self.__logger.error(str(exc_value))
    

