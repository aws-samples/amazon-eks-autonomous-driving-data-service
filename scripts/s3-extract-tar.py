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

import os
import tarfile
import time
import json
import math
import sys

import boto3
from boto3.s3.transfer import TransferConfig
from multiprocessing import Process
import logging

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)

class DownloadProgress(object):
    def __init__(self, size):
        self._size = size
        self._seen_so_far = 0
        self._start = time.time()
        self._prev = 0.0

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percentage = round((self._seen_so_far / self._size) * 100, 2)
        if (percentage - self._prev > 5):
            self._prev = percentage
            elapsed = time.time() - self._start
            S3TarExtractor.logger.info(f'percentage completed... {percentage} in {elapsed} secs')

class S3TarExtractor(Process):
    # class variables
    logger = logging.getLogger("s3-extract-tar")
    P_CONCURRENT = 4
    FILE_CHUNK_COUNT = 20000
    GB = 1024**3
    S3_MAX_IO_QUEUE = 20000
    S3_IO_CHUNKSIZE = 262144
    PUT_RETRY = 5

    def __init__(self,
                config=None,
                index=None,
                count=None):

        Process.__init__(self)

        self.config = config
        self.__pindex = index
        self.__pcount = count
        self.s3_client = boto3.client('s3') 

    def run(self):
        file_path = self.config['file_path']
        ext = os.path.splitext(file_path)[1]
        mode = "r:gz" if ext == ".gz" else "r:bz2" if ext == ".bz2" else "r:xz" if ext == ".xz" else "r"
        tar_file = tarfile.open(name=file_path, mode=mode)
        start = self.config['start']
        end = self.config['end']

        info_list = tar_file.getmembers()
        file_info_list = [ info for info in info_list if info.isfile()]
        file_info_list = file_info_list[start:end] if  end > start else file_info_list
        file_count = len(file_info_list)

        p_chunk = math.ceil(file_count/self.__pcount)
        p_start = self.__pindex * p_chunk
        p_end = min(p_start + p_chunk, file_count)

        file_info_list = file_info_list[p_start:p_end]
        file_count = len(file_info_list)

        start_time = time.time()
        files_extracted=0

        dest_bucket = self.config["dest_bucket"]
        dest_prefix = self.config["dest_prefix"]

        self.logger.info(f"worker {self.__pindex}, start: {start+p_start}, end: {start+p_end}, count: {file_count}")
        for file_info in file_info_list:
            nattempt = 0
            while nattempt < self.PUT_RETRY:
                try:
                    file_reader = tar_file.extractfile(file_info)
                    self.s3_client.put_object(Body=file_reader, 
                                Bucket=dest_bucket, 
                                Key = self.__dest_key(dest_prefix, file_info.name))
                    file_reader.close()
                    files_extracted += 1
                    break
                except Exception as err:
                    self.logger.info(f"Exception: {err}")
                    nattempt += 1
                    time.sleep(nattempt)
            if nattempt >= self.PUT_RETRY:
                self.logger.info(f'worker {self.__pindex}, failed: {file_path}')
                sys.exit(1)

            if files_extracted % 100 == 0:
                elapsed = time.time() - start_time
                self.logger.info(f"worker {self.__pindex}: files extracted: {files_extracted}: {elapsed}s")

        elapsed = time.time() - start_time
        self.logger.info(f"worker {self.__pindex}: files extracted: {files_extracted}: {elapsed}s")

    @classmethod
    def __submit_batch_jobs(cls, config=None, total_file_count=None):

        batch_client = boto3.client('batch')
    
        # batch jobs
        jobs=[]
    
        s3_python_script = config['s3_python_script']
        s3_json_config = config['s3_json_config']
        file_path = config['file_path']
        aws_region = os.environ['AWS_DEFAULT_REGION']

        job_ts = str(time.time()).replace('.','-')
        for i in range(0, total_file_count, cls.FILE_CHUNK_COUNT):
            start = i
            end = min(i + cls.FILE_CHUNK_COUNT, total_file_count)

            response = batch_client.submit_job(
                jobName=f'extract-{start}-{end}-{job_ts}',
                jobQueue=config['job_queue'],
                jobDefinition=config['job_definition'],
                retryStrategy={'attempts': 1},
                containerOverrides={
                    'command': ['--file-path', f'{file_path}', '--start', f'{start}', '--end', f'{end}'],
                    'environment': [
                        {
                            'name': 'S3_PYTHON_SCRIPT',
                            'value': s3_python_script
                        },
                        {
                            'name': 'S3_JSON_CONFIG',
                            'value': s3_json_config
                        },
                        {
                            'name': 'AWS_DEFAULT_REGION',
                            'value': aws_region
                        }
                    ]
                })
            jobs.append(response["jobId"])

        succeeded=[]
        failed = []
        pending=[ job_id for job_id in jobs ]

        while pending:
            response = batch_client.describe_jobs(jobs=pending)
            pending = []

            for _job in response["jobs"]:
                if _job["status"] == 'SUCCEEDED':
                    succeeded.append(_job["jobId"])
                elif _job["status"] == 'FAILED':
                    failed.append(_job["jobId"])
                else:
                    pending.append(_job["jobId"])

            time.sleep(5)
        
        if failed:
            import sys
            sys.exit(f"Failed: batch jobs: {failed}")

    
    @classmethod
    def __dest_key(cls, dest_prefix, member_name):
        dest_key = member_name

        if dest_key.startswith("./"):
            dest_key = dest_key[2:]
        elif dest_key.startswith("/"):
            dest_key = dest_key[1:]
                
        return f'{dest_prefix}/{dest_key}'

    @classmethod
    def __download_file(cls, bucket_name=None, key=None, dir=None):
        file_name = key if key.find('/') == -1 else key.rsplit('/', 1)[1]
        file_path = os.path.join(dir, file_name)

        if os.path.isfile(file_path):
            file_size = os.stat(file_path).st_size
            cls.logger.info(f"Skipping download: s3://{bucket_name}/{key}, file exists: {file_path}, size:{file_size}")
        else:
            s3_client = boto3.client('s3')
            
            with open(file_path, 'wb') as data:
                start_time = time.time()*1000
                tar_size = s3_client.head_object(Bucket=bucket_name, Key=key).get('ContentLength')
                cls.logger.info(f"Begin download: s3://{bucket_name}/{key} : {tar_size} bytes")
                config = TransferConfig(multipart_threshold=cls.GB, 
                    multipart_chunksize=cls.GB,
                    max_io_queue=cls.S3_MAX_IO_QUEUE, 
                    io_chunksize=cls.S3_IO_CHUNKSIZE)
                s3_client.download_fileobj(bucket_name, key, data, Config=config, Callback=DownloadProgress(tar_size))
                data.close()
                elapsed = time.time()*1000 - start_time
                file_size = os.stat(file_path).st_size
                cls.logger.info(f"Download completed: {file_path}: {file_size} bytes in {elapsed} ms")

        return file_path


    @classmethod
    def extract_tar(cls, config=None):

        key = config['key']
        if key:
            tmp_dir = config["tmp_dir"]
            os.makedirs(tmp_dir, mode=0o777, exist_ok=True)
    
            source_bucket = config["source_bucket"]
            config['file_path'] = cls.__download_file(bucket_name=source_bucket, key=key, dir=tmp_dir)

        file_path = config['file_path']
        if not file_path:
            cls.logger.info("config['file_path'] is required")
            sys.exit(1)

        start_time = time.time()*1000

        start = config['start']
        end = config['end']

        total_file_count = 0
        if not start and not end:
            ext = os.path.splitext(file_path)[1]
            mode = "r:gz" if ext == ".gz" else "r:bz2" if ext == ".bz2" else "r:xz" if ext == ".xz" else "r"
            tar_file = tarfile.open(name=file_path, mode=mode)
            info_list = tar_file.getmembers()
            file_info_list = [ info for info in info_list if  info.isfile()]
            total_file_count = len(file_info_list)

        if total_file_count > cls.FILE_CHUNK_COUNT:
            cls.logger.info(f"submit batch jobs for {file_path} : {total_file_count}")
            cls.__submit_batch_jobs(config, total_file_count)
        else:
            _p_list = []
            for i in range(cls.P_CONCURRENT):
                p = S3TarExtractor(config=config, index=i, count=cls.P_CONCURRENT)
                _p_list.append(p)
                p.start()

            for _p in _p_list:
                _p.join()
    
        elapsed = time.time()*1000 - start_time
    
        if not start and not end:
            os.remove(file_path)
            cls.logger.info(f"Total time: {elapsed} ms")    

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract archive from S3 to EFS')
    parser.add_argument('--config', type=str,  help='Configuration JSON file', required=True)
    parser.add_argument('--key', type=str,  default='', help='S3 key for Tar file', required=False)
    parser.add_argument('--file-path', type=str,  default='', help='File path for downloaded file', required=False)
    parser.add_argument('--start', type=int,  default=0, help='Start file index in the archive', required=False)
    parser.add_argument('--end', type=int,  default=0, help='End file index in the archive', required=False)
   
    args = parser.parse_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    config['key'] = args.key
    config['file_path'] = args.file_path

    config['start'] = args.start
    config['end'] = args.end
    
    S3TarExtractor.extract_tar(config=config)
