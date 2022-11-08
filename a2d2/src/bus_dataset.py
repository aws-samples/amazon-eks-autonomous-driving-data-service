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

import logging
import json
from db_reader import DatabaseReader
from threading import Thread


class BusDataset():
    def __init__(self, dbconfig=None, **request):
        self.logger = logging.getLogger("bus_dataset")
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        self.dbreader = DatabaseReader(dbconfig)
        self.dbreader.connect()

        self.cur_batch = None
        self.next_batch = None
        self.pre_fetch_thread = None

        self.vehicle_id = request['vehicle_id']
        self.scene_id = request['scene_id']
        self.start_ts = int(request['start_ts'])
        self.stop_ts = int(request['stop_ts'])
        self.step = int(request['step'])

        self.fetch()
        self.pre_fetch_thread.join()

    def is_open(self):
        return (self.start_ts < self.stop_ts) or self.pre_fetch_thread or self.next_batch
        
    def read(self, query=None):
        self.next_batch=self.dbreader.query(query)

    def fetch(self):
        if self.pre_fetch_thread:
            self.pre_fetch_thread.join()

        self.end_ts = self.start_ts + self.step
        if self.end_ts > self.stop_ts:
            self.end_ts = self.stop_ts

        self.pre_fetch_thread = None
        self.cur_batch=self.next_batch
        self.next_batch=None

        if self.start_ts < self.stop_ts:
            query = '''select * from a2d2.bus_data where vehicle_id = 
                '{0}' and scene_id = '{1}'  and data_ts >= {2}
                AND data_ts < {3} order by data_ts;'''.format(self.vehicle_id,
                    self.scene_id,  self.start_ts, self.end_ts)
            t=Thread(target=self.read, kwargs={"query": query})
            self.pre_fetch_thread = t
            t.start()
        self.start_ts = self.end_ts
        return self.cur_batch

def main(config):
    m = BusDataset(dbconfig=config, 
		vehicle_id="a2d2",
		scene_id="20190401121727",
		start_ts=1554115465612291, 
		stop_ts=1554115466612291,
                step=1000000)

    print(m.fetch())
        
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bus dataset')
    parser.add_argument('--config', type=str,  help='configuration file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config)
