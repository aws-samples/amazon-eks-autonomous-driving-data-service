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

def main():
    client = boto3.client("ssm")
    stack_name = os.getenv('cfn_stack_name')
    path = f'/{stack_name}/'

    response = client.get_parameters_by_path(Path=path)
   
    with open("setenv.sh", "w") as file:
        file.write("#!/bin/bash\n")
        while True:
            parameters = response['Parameters']
            for parameter in parameters:
                name = parameter['Name'].rsplit('/', 1)[1]
                value = parameter['Value']
                
                file.write(f'export {name}={value}\n')
            next_token = response.get('NextToken', None)
            if next_token:
                response = client.get_parameters_by_path(Path=path, NextToken=next_token)
            else:
                break
        file.close()

if __name__ == "__main__":
    main()