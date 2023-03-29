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
[[ -z "${ros1_batch_job_definition}" ]] && echo "ros1_batch_job_definition variable required" && exit 1
[[ -z "${stepfunctions_role_arn}" ]] && echo "stepfunctions_role_arn variable required" && exit 1
[[ -z "${glue_job_role_arn}" ]] && echo "glue_job_role_arn variable required" && exit 1
[[ -z "${redshift_cluster_role_arn}" ]] && echo "redshift_cluster_role_arn variable required" && exit 1
[[ -z "${redshift_cluster_host}" ]] && echo "redshift_cluster_host variable required" && exit 1
[[ -z "${redshift_cluster_username}" ]] && echo "redshift_cluster_username variable required" && exit 1
[[ -z "${redshift_cluster_dbname}" ]] && echo "redshift_cluster_dbname variable required" && exit 1
[[ -z "${redshift_cluster_password}" ]] && echo "redshift_cluster_password variable required" && exit 1

$scripts_dir/pythonfroms3-ecr-image.sh focal
$scripts_dir/pythonfroms3-ecr-image.sh noetic

# create requirements.txt
cat >$scripts_dir/requirements.txt <<EOL
redshift_connector
pandas
numpy
EOL

aws s3 cp $scripts_dir/fordav_extract_load.py s3://$s3_bucket_name/scripts/fordav_extract_load.py
aws s3 cp $scripts_dir/fordav_extract_tar.py s3://$s3_bucket_name/scripts/fordav_extract_tar.py
aws s3 cp $scripts_dir/fordav_extract_bag.py s3://$s3_bucket_name/scripts/fordav_extract_bag.py
aws s3 cp $scripts_dir/fordav_drive_data.py s3://$s3_bucket_name/scripts/fordav_drive_data.py
aws s3 cp $scripts_dir/fordav_bus_etl_job.py s3://$s3_bucket_name/scripts/fordav_bus_etl_job.py
aws s3 cp $scripts_dir/fordav_bus_etl.py s3://$s3_bucket_name/scripts/fordav_bus_etl.py
aws s3 cp $scripts_dir/setup-redshift-db.py s3://$s3_bucket_name/scripts/setup-redshift-db.py
aws s3 cp $scripts_dir/requirements.txt s3://$s3_bucket_name/scripts/requirements.txt

run_ts=$(date +%s)
tmp_dir="/efs/tmp-${run_ts}"

# Create glue.config 
glue_database="fordav${run_ts}"
s3_glue_output_prefix="glue/${glue_database}/bus"

cat >$DIR/fordav/config/fordav-glue.config <<EOL
{
  "database": "${glue_database}",
  "s3_bucket": "${s3_bucket_name}",
  "s3_output_prefix": "${s3_glue_output_prefix}",
  "s3_storage_prefix": "fordav/data",
  "glue_role": "${glue_job_role_arn}",
  "script_location": "s3://${s3_bucket_name}/scripts/fordav_bus_etl.py"
}
EOL
chown ubuntu:ubuntu $DIR/fordav/config/fordav-glue.config

aws s3 cp $DIR/fordav/data/sensors.csv s3://${s3_bucket_name}/fordav/redshift/sensors.csv
aws s3 cp $DIR/fordav/data/vehicle.csv s3://${s3_bucket_name}/fordav/redshift/vehicle.csv

drive_data_output="fordav/redshift/drive/${run_ts}"

# Create redshift.configs
cat >$DIR/fordav/config/fordav-redshift.config <<EOL
{
  "host": "${redshift_cluster_host}",
  "user": "${redshift_cluster_username}",
  "dbname": "${redshift_cluster_dbname}",
  "password": "${redshift_cluster_password}",
  "queries": [
    "CREATE SCHEMA IF NOT EXISTS fordav",
    "CREATE TABLE IF NOT EXISTS fordav.sensor ( sensorid VARCHAR(255) NOT NULL ENCODE lzo, description VARCHAR(255) ENCODE lzo, PRIMARY KEY (sensorid)) DISTSTYLE ALL",
    "CREATE TABLE IF NOT EXISTS fordav.vehicle ( vehicleid VARCHAR(255) NOT NULL ENCODE lzo ,description VARCHAR(255) ENCODE lzo ,PRIMARY KEY (vehicleid)) DISTSTYLE ALL",
    "CREATE TABLE IF NOT EXISTS fordav.drive_data ( vehicle_id varchar(255) encode Text255 not NULL, scene_id varchar(255) encode Text255 not NULL, sensor_id varchar(255) encode Text255 not NULL, data_ts BIGINT not NULL sortkey, s3_bucket VARCHAR(255) encode lzo NOT NULL, s3_key varchar(255) encode lzo NOT NULL, primary key(vehicle_id, scene_id, sensor_id, data_ts), FOREIGN KEY(vehicle_id) references fordav.vehicle(vehicleid), FOREIGN KEY(sensor_id) references fordav.sensor(sensorid)) DISTSTYLE AUTO",
    "CREATE TABLE IF NOT EXISTS fordav.bus_data ( vehicle_id varchar(255) encode Text255 not NULL, scene_id varchar(255) encode Text255 not NULL, data_ts BIGINT not NULL sortkey, angular_velocity_x FLOAT4 not NULL, angular_velocity_y FLOAT4 not NULL, angular_velocity_z FLOAT4 not NULL, linear_acceleration_x FLOAT4 not NULL, linear_acceleration_y FLOAT4 not NULL, linear_acceleration_z FLOAT4 not NULL,  latitude FLOAT4 not NULL, longitude FLOAT4 not NULL, altitude FLOAT4 not NULL,  primary key(vehicle_id, scene_id, data_ts), FOREIGN KEY(vehicle_id) references fordav.vehicle(vehicleid) ) DISTSTYLE AUTO",
    "DELETE FROM fordav.bus_data",
    "DELETE FROM fordav.drive_data",
    "DELETE FROM fordav.sensor",
    "DELETE FROM fordav.vehicle",
    "COPY fordav.sensor FROM 's3://${s3_bucket_name}/fordav/redshift/sensors.csv' iam_role  '${redshift_cluster_role_arn}' CSV",
    "COPY fordav.vehicle FROM 's3://${s3_bucket_name}/fordav/redshift/vehicle.csv' iam_role  '${redshift_cluster_role_arn}' CSV",
    "COPY fordav.drive_data FROM 's3://${s3_bucket_name}/${drive_data_output}/' iam_role  '${redshift_cluster_role_arn}' CSV",
    "COPY fordav.bus_data FROM 's3://${s3_bucket_name}/${s3_glue_output_prefix}/' iam_role  '${redshift_cluster_role_arn}' CSV IGNOREHEADER 1"
  ]
}
EOL
chown ubuntu:ubuntu $DIR/fordav/config/fordav-redshift.config


# Create fordav-data.config 
cat >$DIR/fordav/config/fordav-data.config <<EOL
{
  "source_bucket": "ford-multi-av-seasonal",
  "source_prefix": "",
  "dest_bucket": "${s3_bucket_name}",
  "dest_prefix": "fordav/data",
  "drive_data_prefix": "${drive_data_output}",
  "job_definition": "${batch_job_definition}",
  "ros1_job_definition": "${ros1_batch_job_definition}",
  "job_queue": "${batch_job_queue}",
  "tar_s3_python_script": "s3://${s3_bucket_name}/scripts/fordav_extract_tar.py",
  "bag_s3_python_script": "s3://${s3_bucket_name}/scripts/fordav_extract_bag.py",
  "s3_json_config": "s3://${s3_bucket_name}/config/fordav-data.config",
  "tmp_dir": "${tmp_dir}",
  "ros_package_name": "fordav_pointcloud",
  "ros_package_launch": "fordav_pointcloud.launch"
}
EOL

aws s3 cp $DIR/fordav/config/fordav-data.config s3://$s3_bucket_name/config/fordav-data.config
aws s3 cp $DIR/fordav/config/fordav-glue.config s3://$s3_bucket_name/config/fordav-glue.config
aws s3 cp $DIR/fordav/config/fordav-redshift.config s3://$s3_bucket_name/config/fordav-redshift.config

job_name1="fordav-extract-load-${run_ts}"
job_name2="fordav-drive-data-${run_ts}"
job_name3="fordav-bus-data-${run_ts}"
job_name4="fordav-redshift-data-${run_ts}"

# Create fordav-sfn.configs 
cat >$DIR/fordav/config/fordav-sfn.config <<EOL
{
  "role_arn": "${stepfunctions_role_arn}",
  "definition": {
    "Comment": "Steps for fordav data processing workflow",
    "StartAt": "FordavExtractLoad",
    "States": {
      "FordavExtractLoad": {
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
                "Value": "s3://${s3_bucket_name}/scripts/fordav_extract_load.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/fordav-data.config"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 172800}
        },
        "Next": "FordavDriveData"
      },
      "FordavDriveData": {
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
                "Value": "s3://${s3_bucket_name}/scripts/fordav_drive_data.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/fordav-data.config"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 36000}
        },
        "Next": "FordavBusData"
      },
      "FordavBusData": {
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
                "Value": "s3://${s3_bucket_name}/scripts/fordav_bus_etl_job.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/fordav-glue.config"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 36000}
        },
        "Next": "FordavRedshiftData"
      },
      "FordavRedshiftData": {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Parameters": {
          "JobQueue": "${batch_job_queue}",
          "JobDefinition": "${batch_job_definition}",
          "JobName": "${job_name4}",
          "ContainerOverrides": {
            "Environment": [
              {
                "Name": "S3_PYTHON_SCRIPT",
                "Value": "s3://${s3_bucket_name}/scripts/setup-redshift-db.py"
              },
              {
                "Name": "S3_JSON_CONFIG",
                "Value": "s3://${s3_bucket_name}/config/fordav-redshift.config"
              },
              {
                "Name": "S3_REQUIREMENTS_TXT",
                "Value": "s3://${s3_bucket_name}/scripts/requirements.txt"
              }
            ] 
          },
          "RetryStrategy": {"Attempts": 2},
          "Timeout": {"AttemptDurationSeconds": 36000}
        },
        "End": true
      }
    }
  }
}
EOL

python3 $scripts_dir/step-functions.py --config $DIR/fordav/config/fordav-sfn.config