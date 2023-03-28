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
import sys
import os
  
current = os.path.dirname(os.path.realpath(__file__))
adds = os.path.join(os.path.dirname(os.path.dirname(current)), "adds",  "src")
sys.path.append(adds)

from kafka_data_client import DataClient

import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka data client')
    parser.add_argument('--config', type=str, help='Kafka data client JSON config file path', required=True)
    
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)
        json_file.close()

        data_client = DataClient(config)
        data_client()
        data_client.close()
        sys.exit()
        