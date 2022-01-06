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
import random
import string
import os
import stat
import errno

import boto3

def get_s3_client():
    s3_client = None
    try:
        session = boto3.session.Session()
        s3_client = session.client('s3')
    except Exception as e:
        try:
            print(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])
            print(os.environ['AWS_ROLE_ARN'])
            s3_client = boto3.client('s3')
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))

    assert(s3_client != None)
    return s3_client

def get_s3_resource():
    s3_resource = None
    try:
        session = boto3.session.Session()
        s3_resource = session.resource('s3')
    except Exception as e:
        try:
            print(os.environ['AWS_WEB_IDENTITY_TOKEN_FILE'])
            print(os.environ['AWS_ROLE_ARN'])
            s3_resource = boto3.resource('s3')
        except Exception as e:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=20, file=sys.stdout)
            print(str(e))

    return s3_resource

def random_string(length=16):
    s = ''
    sel = string.ascii_lowercase + string.ascii_uppercase + string.digits
    for _ in range(0, length):
        s += random.choice(sel)
    return s

def is_close_msg(json_msg):
    close = False

    try:
        close = json_msg['__close__']
    except KeyError:
        pass

    return close

def mkdir_p(path):
    try:
        os.makedirs(path)
        os.chmod(path, stat.S_IROTH|stat.S_IWOTH|stat.S_IXOTH|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
