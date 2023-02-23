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
from typing import Any

import redshift_connector
import boto3
import time

import sys, traceback
import logging
import json

class DatabaseReader:
    MAX_ATTEMPTS = 5

    def __init__(self, dbconfig: dict):
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.__logger = logging.getLogger("dbreader")

        self.__dbconfig = dbconfig
    
    def connect(self):
        try:
            dbname = self.__dbconfig["dbname"]
            host = self.__dbconfig["host"]
            user = self.__dbconfig["user"]
            password = self.__dbconfig["password"]

            secrets_client = boto3.client('secretsmanager')
            response = secrets_client.get_secret_value(SecretId=password)
            password = response['SecretString']

            parts = host.split('.')
            serverless_work_group=parts[0]
            serverless_acct_id=parts[1]

            self.con=redshift_connector.connect(database=dbname, host=host, user=user, password=password, 
                is_serverless=True, serverless_acct_id=serverless_acct_id, serverless_work_group=serverless_work_group)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

    def query(self, query:str) -> Any:
        result = None
        cur = None
        attempt = 0
        while True:
            try:
                cur = self.con.cursor()
                cur.execute(query)
        
                result: tuple = cur.fetchall()
                break
            except redshift_connector.error.ProgrammingError as e:
                if "no result set" in str(e):
                    self.con.commit()
                    break

                if attempt <= self.MAX_ATTEMPTS:
                    attempt += 1
                    self.__logger.warning(f"{e}; retrying: {attempt}")
                    time.sleep(2**attempt)
                else:
                    raise(e)

        if cur:
            cur.close()

        return result

    def close(self):
        try:
            if self.con:
                self.con.close()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            self.__logger.error(str(exc_type))
            self.__logger.error(str(exc_value))

def main(config, query):
    db = DatabaseReader(dbconfig=config["database"])
    db.connect()
    print(db.query(query))
    db.close()


import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DatabaseReader thread')
    parser.add_argument('--config', type=str,  help='configuration file', required=True)
    parser.add_argument('--query', type=str,  help='query', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config, args.query)
