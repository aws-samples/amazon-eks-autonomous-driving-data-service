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
import os

from util import get_s3_client, random_string

class S3Reader(Process):
    def __init__(self, req=None, resp=None):
        Process.__init__(self)
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("s3_reader")

        self._req = req
        self._resp = resp
        self._s3_client = get_s3_client()
        self.tmp = os.getenv("TMP", default="/tmp")

    def run(self):

        while True:
            try:
                msg = self._req.get(block=True)
                if msg == "__close__":
                    break

                s3_info = msg.split(" ", 1)
                bucket = s3_info[0]
                key = s3_info[1]
                file_path = os.path.join(self.tmp, random_string())
                self._s3_client.download_file(bucket, key, file_path)
                self._resp.put(file_path + " " + bucket + " " + key)
            except Exception as _:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
                self.logger.error(str(exc_type))
                self.logger.error(str(exc_value))

        self.logger.info("exit")
    

