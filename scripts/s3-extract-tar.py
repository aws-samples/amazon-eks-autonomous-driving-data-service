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

import boto3
import os
import tarfile
import time
from boto3.s3.transfer import TransferConfig

from multiprocessing import Process
import logging

class S3TarExtractor(Process):
    def __init__(self, 
                tar_path=None, 
                dest_bucket=None, 
                dest_prefix=None, 
                process_index=None, 
                process_count=None):

        Process.__init__(self)
        self.process_index = process_index
        self.process_count = process_count

        logging.basicConfig(
            format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("s3-extract-tar")

        self.tar_path = tar_path
        self.dest_bucket = dest_bucket
        self.dest_prefix = dest_prefix[:-1] if dest_prefix.endswith('/') else dest_prefix
        
        session = boto3.session.Session()
        self.s3_client = session.client('s3')
    
    def _is_tar_extracted(self):
        _extracted = True
        
        try:
            self.logger.info(f'Checking: s3://{self.dest_bucket}/{self.dest_prefix}/, process index: {self.process_index}')
            file_index = 0

            with tarfile.open(self.tar_path) as tar_obj:
                while True:
                    member = tar_obj.next()
                    if not member:
                        break
                    
                    if (file_index % self.process_count == self.process_index) and member.isfile():
                        key = self._dest_key(member.name)
                        response = self.s3_client.head_object(Bucket=self.dest_bucket, Key=key)
                        if response['ContentLength'] != member.size:
                            _extracted = False
                            break
                    file_index += 1

        except Exception as e:
            print(e)
            _extracted = False

        return _extracted

    def _dest_key(self, member_name):
        dest_key = member_name

        if dest_key.startswith("./"):
            dest_key = dest_key[2:]
        elif dest_key.startswith("/"):
            dest_key = dest_key[1:]
        
        return f'{self.dest_prefix}/{dest_key}'

    def run(self):
        file_index = 0

        if self._is_tar_extracted():
            self.logger.info(f'Skipping: {self.tar_path}, process index: {self.process_index}')
            return

        self.logger.info(f'extracting: {self.tar_path}, process index: {self.process_index}')
        with tarfile.open(self.tar_path) as tar_obj:
            while True:
                member = tar_obj.next()
                if not member:
                    break

                if (file_index % self.process_count == self.process_index) and member.isfile():
                    nattempt = 0
                    while nattempt < 3:
                        try:
                            file_reader = tar_obj.extractfile(member)
                            self.s3_client.put_object(Body=file_reader, 
                                Bucket=self.dest_bucket, 
                                Key=self._dest_key(member.name))
                            file_reader.close()
                            break
                        except Exception:
                            nattempt += 1
                            time.sleep(nattempt)
                    if nattempt >= 3:
                        import sys
                        self.logger.error(f'failed: {self.tar_path}, process index: {self.process_index}')
                        sys.exit(1)
                    
                file_index += 1

        self.logger.info(f'completed: {self.tar_path}, process index: {self.process_index}')

def main(args):
    

    s3_client = boto3.client('s3')
    source_bucket = args.source_bucket
    source_key = args.source_key
    dest_bucket = args.dest_bucket
    dest_prefix = args.dest_prefix
    file_name = source_key if source_key.find('/') == -1 else source_key.rsplit('/', 1)[1] 

    tmpdir = os.getenv("TMP", "/tmp")
    file_path = os.path.join(tmpdir, file_name)
    with open(file_path, 'wb') as data:
        print(f"downloading: s3://{source_bucket}/{source_key}")
        start = time.time()
        s3_client.download_fileobj(source_bucket, source_key, data, Config=TransferConfig())
        print(f"downloaded: s3://{source_bucket}/{source_key} --> {file_path} {time.time() - start} s")
    data.close()

    if tarfile.is_tarfile(file_path):
        p_count = args.process_count
        p_list = []
        for p_index in range(p_count):
            p = S3TarExtractor(tar_path=file_path, 
                    dest_bucket=dest_bucket, 
                    dest_prefix=dest_prefix,
                    process_index=p_index, 
                    process_count=p_count)
            p.start()
            p_list.append(p)
        
        for i in range(p_count):
            p_list[i].join()
 
        try:
            print(f"deleting: {file_path}")
            os.remove(file_path)
        except Exception:
            pass
        
    else:
        raise ValueError(f'Not a TAR file: s3://{source_bucket}/{source_key}')

    elapsed = time.time() - start
    print(f"s3://{source_bucket}/{source_key}: process count: {p_count}: total time: {elapsed} secs")
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract and Load S3 tar file')
    parser.add_argument('--source-bucket', type=str,  help='S3 source bucket', required=True)
    parser.add_argument('--dest-bucket', type=str,  help='S3 dest bucket', required=True)
    parser.add_argument('--source-key', type=str,  help='S3 source object key', required=True)
    parser.add_argument('--dest-prefix', type=str,  help='S3 dest prefix', required=True)
    parser.add_argument('--process-count', type=int,  default=8, help='concurrent process count', required=False)
    
    args = parser.parse_args()
    main(args)
