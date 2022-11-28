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

from common.db_reader import DatabaseReader
from threading import Thread


class BusDataset():
    def __init__(self, dbconfig=None, **request):
        self.__dbreader = DatabaseReader(dbconfig)
        self.__dbreader.connect()

        self.__cur_batch = None
        self.__next_batch = None
        self.__pre_fetch_thread = None

        self.__vehicle_id = request['vehicle_id']
        self.__scene_id = request['scene_id']
        self.__start_ts = int(request['start_ts'])
        self.__stop_ts = int(request['stop_ts'])
        self.__step = int(request['step'])

        self.fetch()
        self.__pre_fetch_thread.join()

    def is_open(self):
        return (self.__start_ts < self.__stop_ts) or self.__pre_fetch_thread or self.__next_batch
        
    def read(self, query=None):
        self.__next_batch=self.__dbreader.query(query)

    def fetch(self):
        if self.__pre_fetch_thread:
            self.__pre_fetch_thread.join()

        self.end_ts = self.__start_ts + self.__step
        if self.end_ts > self.__stop_ts:
            self.end_ts = self.__stop_ts

        self.__pre_fetch_thread = None
        self.__cur_batch=self.__next_batch
        self.__next_batch=None

        if self.__start_ts < self.__stop_ts:
            query = '''select * from a2d2.bus_data where vehicle_id = 
                '{0}' and scene_id = '{1}'  and data_ts >= {2}
                AND data_ts < {3} order by data_ts;'''.format(self.__vehicle_id,
                    self.__scene_id,  self.__start_ts, self.end_ts)
            t=Thread(target=self.read, kwargs={"query": query})
            self.__pre_fetch_thread = t
            t.start()
        self.__start_ts = self.end_ts
        return self.__cur_batch
