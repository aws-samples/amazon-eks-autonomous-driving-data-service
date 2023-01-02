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
import redshift_connector
import sys, traceback
import logging
import json

import time
import boto3

MAX_ATTEMPTS = 5

logger = logging.getLogger("data_service")
logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)

def execute(con=None, query=None):
        cur=None
        attempt = 0
        while True:
            try:
                cur = con.cursor()
                cur.execute("begin;")
                cur.execute(query)
                cur.execute("commit;")
                break
            except redshift_connector.error.ProgrammingError as e:
                if attempt <= MAX_ATTEMPTS:
                    attempt += 1
                    logger.warning(f"{e}; retrying: {attempt}")
                    time.sleep(2**attempt)
                else:
                    raise(e)

        if cur:
            cur.close()

def main(config):
    con = None

    try:
        dbname = config["dbname"]
        host = config["host"]
        user = config["user"]
        password = config["password"]

        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(SecretId=password)
        password = response['SecretString']

        parts = host.split('.')
        serverless_work_group=parts[0]
        serverless_acct_id=parts[1]

        con=redshift_connector.connect(database=dbname, host=host, 
            user=user, password=password, 
            is_serverless=True, serverless_acct_id=serverless_acct_id, 
            serverless_work_group=serverless_work_group)

        for query in config['queries']:
            logger.info(f"Executing {query}")
            execute(con=con, query=query)
          
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
        logger.error(str(exc_type))
        logger.error(str(exc_value))
        raise RuntimeError(str(e))
    finally:
        if con:
            con.close()
    
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Redshift configuration for autonomous driving data service')
    parser.add_argument('--config', type=str,  help='Redshift configuration JSON file', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config)
