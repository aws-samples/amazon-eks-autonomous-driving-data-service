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

from threading import Thread
from logging import Logger

def join_thread_timeout_retry(name: str, t: Thread, timeout: float, max_retry: int, logger: Logger = None):
    """Join a thread with timeout and max retries. 

    Parameters
    ----------
    name: str
        Thread name
    t: Thread
        Thread to join
    timeout: float
        Join timeout in seconds
    max_retry: int
        Max retries
    logger; Logger
        Logger for logging
    """
     
    attempt  = 0
    while t.is_alive() and attempt < max_retry:
        attempt += 1
        t.join(timeout=timeout)
        if t.is_alive():
            if logger:
                logger.warning(f"Thread {name} timeout: {timeout}, attempt: {attempt}")
       
    if t.is_alive():
        if logger:
            logger.error(f"Thread {name} timeout: {timeout}, attempt: {attempt}")
        raise RuntimeError(f"Thread {name} is still running after max retry")
