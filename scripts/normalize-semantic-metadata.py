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
import json
import re

def s3_bucket_keys(s3_client, bucket, prefix, input_regex=None):
    """Generator for listing S3 bucket keys matching prefix and suffix"""

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if not input_regex or re.match(input_regex, key):
                yield key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def normalize_semantic_metadata(config):

    bucket = config["s3_bucket"]
    input_prefix = config["s3_input_prefix"]
    if input_prefix.endswith("/"):
        input_prefix = input_prefix[:-1]

    input_regex = f"{input_prefix}(/([0-9_]+)/camera/cam_([A-Za-z0-9_]+)/[0-9]+_camera_[A-Za-z0-9_]+[.]json)"
    
    s3_client = boto3.client(service_name='s3')
    JSON_EXT = ".json"
    PNG_EXT = ".png"
    NPZ_EXT = ".npz"

    nkeys = 0
    for key in s3_bucket_keys(s3_client, bucket=bucket, prefix=input_prefix, input_regex=input_regex):
       
        json_data = s3_client.get_object(Bucket=bucket, Key=key)
        json_obj = json.loads(json_data['Body'].read())

        match = re.match(input_regex, key)
        base_path = match.group(1)
        scene_id = match.group(2).replace('_', '')

        json_obj['vehicle_id'] = "a2d2/semantic"
        json_obj['scene_id'] = scene_id
      
        json_obj['image_png'] = input_prefix +  base_path.replace(JSON_EXT, PNG_EXT)
        response = s3_client.head_object(Bucket=bucket, Key=json_obj['image_png'])
        assert(  int(response['ContentLength']) )
        
        json_obj['pcld_npz'] = input_prefix + base_path.replace('camera', 'lidar').replace(JSON_EXT, NPZ_EXT)
        response = s3_client.head_object(Bucket=bucket, Key=json_obj['pcld_npz'])
        assert(  int(response['ContentLength']) )

        json_obj['label3D_json'] = input_prefix + base_path.replace('camera', 'label3D')
        response = s3_client.head_object(Bucket=bucket, Key=json_obj['label3D_json'])
        assert(  int(response['ContentLength']) )

        json_obj['label_png'] = input_prefix + base_path.replace('camera', 'label').replace(JSON_EXT, PNG_EXT)
        response = s3_client.head_object(Bucket=bucket, Key=json_obj['label_png'])
        assert(  int(response['ContentLength']) )

        s3_client.put_object(Body=json.dumps(json_obj), Bucket=bucket, Key=key)

        nkeys += 1

        if nkeys % 100 == 0:
            print(f"Processed {nkeys} keys")


def main(config):
    # normalize semantic  metadata
    normalize_semantic_metadata(config)
    
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Normalize semantic bboxes json')
    parser.add_argument('--config', type=str,  help='Normalize semantic bboxes json', required=True)
    
    args, _ = parser.parse_known_args()

    with open(args.config) as json_file:
        config = json.load(json_file)

    main(config)

