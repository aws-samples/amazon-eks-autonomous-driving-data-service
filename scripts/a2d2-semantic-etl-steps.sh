#!/bin/bash
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
#Permission is hereby granted, free of charge, to any person obtaining a copy of this
#software and associated documentation files (the "Software"), to deal in the Software
#without restriction, including without limitation the rights to use, copy, modify,
#merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#permit persons to whom the Software is furnished to do so.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# set s3 bucket name
scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$scripts_dir/..
cd $scripts_dir && python3 get-ssm-params.py && source setenv.sh

[[ -z "${s3_bucket_name}" ]] && echo "s3_bucket_name variable required" && exit 1
[[ -z "${batch_job_queue}" ]] && echo "batch_job_queue variable required" && exit 1
[[ -z "${batch_job_definition}" ]] && echo "batch_job_definition variable required" && exit 1
[[ -z "${stepfunctions_role_arn}" ]] && echo "stepfunctions_role_arn variable required" && exit 1
[[ -z "${glue_job_role_arn}" ]] && echo "glue_job_role_arn variable required" && exit 1
[[ -z "${redshift_cluster_role_arn}" ]] && echo "redshift_cluster_role_arn variable required" && exit 1
[[ -z "${redshift_cluster_host}" ]] && echo "redshift_cluster_host variable required" && exit 1
[[ -z "${redshift_cluster_port}" ]] && echo "redshift_cluster_port variable required" && exit 1
[[ -z "${redshift_cluster_username}" ]] && echo "redshift_cluster_username variable required" && exit 1
[[ -z "${redshift_cluster_dbname}" ]] && echo "redshift_cluster_dbname variable required" && exit 1
[[ -z "${redshift_cluster_password}" ]] && echo "redshift_cluster_password variable required" && exit 1

$scripts_dir/pythonfroms3-focal-ecr-image.sh

# create requirements.txt
cat >$scripts_dir/requirements.txt <<EOL
psycopg2-binary
pandas
numpy
EOL

aws s3 cp $scripts_dir/normalize-semantic-metadata.py s3://$s3_bucket_name/scripts/normalize-semantic-metadata.py
aws s3 cp $scripts_dir/a2d2-semantic-metadata-etl.py s3://$s3_bucket_name/scripts/a2d2-semantic-metadata-etl.py
aws s3 cp $scripts_dir/requirements.txt s3://$s3_bucket_name/scripts/requirements.txt
aws s3 cp $DIR/a2d2/data/sensors-semantic.csv s3://${s3_bucket_name}/redshift/sensors-semantic.csv
aws s3 cp $DIR/a2d2/data/vehicle-semantic.csv s3://${s3_bucket_name}/redshift/vehicle-semantic.csv

tmp_dir="/efs/tmp-$(date +%s)"

# Create normalize-semantic-metadata.config
cat >$DIR/a2d2/config/normalize-semantic-metadata.config <<EOL
{
  "s3_bucket": "${s3_bucket_name}",
  "s3_input_prefix": "a2d2/camera_lidar_semantic_bboxes"
}
EOL
chown ubuntu:ubuntu $DIR/a2d2/config/normalize-semantic-metadata.config

# Create glue-semantic.config 
s3_glue_output_prefix="glue/a2d2-semantic/$(date +%s)"

cat >$DIR/a2d2/config/glue-semantic.config <<EOL
{
  "s3_bucket": "${s3_bucket_name}",
  "s3_output_prefix": "${s3_glue_output_prefix}",
  "glue_role": "${glue_job_role_arn}",
  "script_location": "s3://${s3_bucket_name}/scripts/a2d2-semantic-metadata-etl.py"
}
EOL
chown ubuntu:ubuntu $DIR/a2d2/config/glue-semantic.config
            
# Create redshift.configs
cat >$DIR/a2d2/config/redshift-semantic.config <<EOL
{
  "host": "${redshift_cluster_host}",
  "port": "${redshift_cluster_port}",
  "user": "${redshift_cluster_username}",
  "dbname": "${redshift_cluster_dbname}",
  "password": "${redshift_cluster_password}",
  "queries": [
    "COPY a2d2.sensor FROM 's3://${s3_bucket_name}/redshift/sensors-semantic.csv' iam_role  '${redshift_cluster_role_arn}' CSV",
    "COPY a2d2.vehicle FROM 's3://${s3_bucket_name}/redshift/vehicle-semantic.csv' iam_role  '${redshift_cluster_role_arn}' CSV",
    "COPY a2d2.drive_data FROM 's3://${s3_bucket_name}/${s3_glue_output_prefix}/image/' iam_role  '${redshift_cluster_role_arn}' CSV IGNOREHEADER 1",
    "COPY a2d2.drive_data FROM 's3://${s3_bucket_name}/${s3_glue_output_prefix}/pcld/' iam_role  '${redshift_cluster_role_arn}' CSV IGNOREHEADER 1",
    "COPY a2d2.drive_data FROM 's3://${s3_bucket_name}/${s3_glue_output_prefix}/label3D/' iam_role  '${redshift_cluster_role_arn}' CSV IGNOREHEADER 1",
    "COPY a2d2.drive_data FROM 's3://${s3_bucket_name}/${s3_glue_output_prefix}/label/' iam_role  '${redshift_cluster_role_arn}' CSV IGNOREHEADER 1"
  ]
}
EOL
chown ubuntu:ubuntu $DIR/a2d2/config/redshift-semantic.config

aws s3 cp $DIR/a2d2/config/normalize-semantic-metadata.config s3://$s3_bucket_name/config/normalize-semantic-metadata.config
aws s3 cp $DIR/a2d2/config/glue-semantic.config s3://$s3_bucket_name/config/glue-semantic.config
aws s3 cp $DIR/a2d2/config/redshift-semantic.config s3://$s3_bucket_name/config/redshift-semantic.config

job_name1="a2d2-normalize-semantic-$(date +%s)"
job_name2="a2d2-extract-semantic-$(date +%s)"
job_name3="a2d2-upload-semantic-$(date +%s)"

# Create a2d2-semantic-sfn.configs 
cat >$DIR/a2d2/config/a2d2-semantic-sfn.config <<EOL
{
  "role_arn": "${stepfunctions_role_arn}",
  "definition": {
    "Comment": "Steps for a2d2 semantic metadata ETL workflow",
    "StartAt": "A2D2NormalizeSemantic",
    "States": {
      "A2D2NormalizeSemantic": {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobQueue": "${batch_job_queue}",
          "JobDefinition": "${batch_job_definition}",
          "JobName": "${job_name1}",
          "ContainerOverrides": {
            "Environment": [
              {
                "Name": "S3_PYTHON_SCRIPT",
                "Value": "s3://${s3_bucket_name}/scripts/normalize-semantic-metadata.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/normalize-semantic-metadata.config"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 3600}
        },
        "Next": "A2D2ExtractSemantic"
      },
      "A2D2ExtractSemantic": {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobQueue": "${batch_job_queue}",
          "JobDefinition": "${batch_job_definition}",
          "JobName": "${job_name2}",
          "ContainerOverrides": {
            "Environment": [
              {
                "Name": "S3_PYTHON_SCRIPT",
                "Value": "s3://${s3_bucket_name}/scripts/glue-etl-job.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/glue-semantic.config"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 3600}
        },
        "Next": "A2D2UploadSemantic"
      },
      "A2D2UploadSemantic": {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobQueue": "${batch_job_queue}",
          "JobDefinition": "${batch_job_definition}",
          "JobName": "${job_name3}",
          "ContainerOverrides": {
            "Environment": [
              {
                "Name": "S3_PYTHON_SCRIPT",
                "Value": "s3://$s3_bucket_name/scripts/setup-redshift-db.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://$s3_bucket_name/config/redshift-semantic.config"
              },
              {
                "Name": "S3_REQUIREMENTS_TXT",
                "Value": "s3://$s3_bucket_name/scripts/requirements.txt"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 1800}
        },
        "End": true
      }
    }
  }
}
EOL

python3 $scripts_dir/step-functions.py --config $DIR/a2d2/config/a2d2-semantic-sfn.config