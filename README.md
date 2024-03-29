# Autonomous Driving Data Service (ADDS)

## Overview

ADDS is a [ROS](https://www.ros.org/) based data service for replaying selected drive scenes from multimodal driving datasets. The multimodal dataset used with ADDS is typically gathered during the development of advanced driver assistance systems (ADAS), or autonomous driving systems (ADS), and comprises of 2D image, 3D point cloud and vehicle bus data. 

One common reason for replaying drive data is to visualize the data. ADDS supports visualization of replayed data using any ROS visualization tool, for example [Foxglove studio](https://foxglove.dev/).

ADDS is supported on [ROS 1 `noetic`](http://wiki.ros.org/noetic/Installation), and [ROS 2 `humble`](https://docs.ros.org/en/humble/index.html). ADDS is pre-configured to use [Audi Autonomous Driving Dataset (A2D2)](https://www.a2d2.audi/a2d2/en.html) and [Ford Multi-AV Seasonal Dataset](https://registry.opendata.aws/ford-multi-av-seasonal/). [ADDS can be extended](#extending-adds-to-other-datasets) to other autonomous driving datasets. 

In the following sections, we describe the ADDS [logical dataset design](#logical-dataset-design) and [runtime data services](#runtime-services). This is followed by a [step-by-step tutorial](#step-by-step-tutorial) for building and using ADDS with A2D2 dataset. Finally, we discuss [extending ADDS to other datasets](#extending-adds-to-other-datasets).

## Logical dataset design

Any multimodal dataset used with ADDS is assumed to contain drive data gathered from a *homogeneous* vehicle fleet. By *homogeneous*, we mean that all the vehicles in the fleet have the same vehicle sensor array configuration and vehicle bus data attributes. Each vehicle in the fleet can have distinct calibration data for the sensor array configuration.

Each ADDS runtime instance serves one multimodal dataset. To serve multiple datasets, you need corresponding number of ADDS runtime instances. 

Each multimodal dataset is comprised of multimodal [frame](#frame-data) and [tabular](#tabular-data) data.

### Frame data

The serialized multimodal data acquired in the vehicle in some file-format, for example, [MDF 4](https://www.asam.net/project-detail/asam-mdf-image-radar-lidar-sensor-logging/), [MCAP](https://mcap.dev/), or [Ros bag](http://wiki.ros.org/Bags), must be decomposed into discreet timestamped 2D image and 3D point cloud data frames, and the frames must be stored in an Amazon S3 bucket under some [bucket prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html). 

The workflow for decomposing the serialized data and storing it in S3 is not prescribed by ADDS.  For many public datasets, for example [A2D2: Audi Autonomous Driving Dataset](https://aws.amazon.com/marketplace/pp/prodview-ktq4gcovho2i4?sr=0-1&ref_=beagle&applicationId=AWSMPContessa#resources), and [Ford Multi-AV Seasonal Dataset](https://registry.opendata.aws/ford-multi-av-seasonal/), the serialized data has already been decomposed into discreet data frames. However, for these types of public datasets, one may still need to extract the discreet data frames from compressed archives (e.g. Zip or Tar files), and upload them to the ADDS S3 bucket.  

For the discreet data frames stored in the S3 bucket, we need a fast retrieval mechanism so we can replay the data frames on-demand. For that purpose, we build a data frame *manifest* and store it in an [Amazon Redshift](https://docs.aws.amazon.com/redshift/) database table: This is described in detail in [drive data](#drive-data). The manifest contains pointers to the data frames stored in S3 bucket.  For the A2D2 dataset, the extraction and loading of the [drive data](#drive-data) is done automatically during the [step-by-step tutorial](#step-by-step-tutorial).

The vehicle bus data is stored in an Amazon Redshift table and is described in [vehicle bus data](#vehicle-bus-data). 

### Tabular data

Each logical multimodal dataset must use a distinct [Amazon Redshift named schema](https://docs.aws.amazon.com/redshift/latest/dg/r_Schemas_and_tables.html). Below, we describe the data definition language (DDL) for creating the required Amazon Redshift tables within a given logical dataset's `schema_name`. 

For the A2D2 dataset, we use `a2d2` as the Redshift schema name, and all the required tables are created automatically during the [step-by-step tutorial](#step-by-step-tutorial).

[Amazon Redshift does not impose *Primary* and *Foreign* key constraints](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html). This point is especially important to understand so you can avoid duplication of data in Redshift tables when you [extend ADDS to other datasets](#extending-adds-to-other-datasets).

#### Vehicle data

Vehicle data is stored in `schema_name.vehicle` table. The DDL for the table is shown below::

	CREATE TABLE IF NOT EXISTS schema_name.vehicle
	(
		vehicleid VARCHAR(255) NOT NULL ENCODE lzo,
		description VARCHAR(255) ENCODE lzo,
		PRIMARY KEY (vehicleid)
	)
	DISTSTYLE ALL;

The `vehicleid` refers to the required unique vehicle identifier. The `description` is optional.

For the A2D2 dataset, the vehicle data is automatically loaded into the `a2d2.vehicle` table from [vehicle.csv](a2d2/data/vehicle.csv) during the [step-by-step tutorial](#step-by-step-tutorial).

#### Sensor data

Sensor data is stored in `schema_name.sensor` table. The DDL for the table is shown below::


	CREATE TABLE IF NOT EXISTS schema_name.sensor
	(
		sensorid VARCHAR(255) NOT NULL ENCODE lzo,
		description VARCHAR(255) ENCODE lzo,
		PRIMARY KEY (sensorid)
	)
	DISTSTYLE ALL;

Each `sensorid` must be unique in a dataset and must refer to a sensor in the *homogeneous* vehicle sensor array configuration. The `description` is optional.

The implicit `sensorid` value `Bus` is **reserved** and denotes vehicle bus: This implicit value is *not* stored in the `schema_name.sensor` table.

For the A2D2 dataset, the sensor data is automatically loaded into the `a2d2.sensor` table from [sensors.csv](a2d2/data/sensors.csv) during the [step-by-step tutorial](#step-by-step-tutorial).

#### Drive data

Image and point cloud data frames must be stored in an Amazon S3 bucket. Pointers to the S3 data frames must be stored in the `schema_name.drive_data` table. The DDL for the table is shown below:

	create table
	schema_name.drive_data
	(
		vehicle_id varchar(255) encode Text255 not NULL,
		scene_id varchar(255) encode Text255 not NULL,
		sensor_id varchar(255) encode Text255 not NULL,
		data_ts BIGINT not NULL sortkey,
		s3_bucket VARCHAR(255) encode lzo NOT NULL,
		s3_key varchar(255) encode lzo NOT NULL,
		primary key(vehicle_id, scene_id, sensor_id, data_ts),
		FOREIGN KEY(vehicle_id) references a2d2.vehicle(vehicleid),
		FOREIGN KEY(sensor_id) references a2d2.sensor(sensorid)
	)
	DISTSTYLE AUTO;

The `scene_id` is an arbitrary identifier for a unique drive scene. 

The `data_ts` is the acquisition timestamp for a discreet data frame and is typically measured in [international atomic time (TAI)](https://en.wikipedia.org/wiki/International_Atomic_Time).

The `s3_bucket` is the name of the Amazon S3 bucket, and the `s3_key` is the [key](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingObjects.html) for the data frame stored in the S3 bucket.

For the A2D2 dataset, the drive data is automatically loaded into the `a2d2.drive_data` table during the [step-by-step tutorial](#step-by-step-tutorial).

#### Vehicle bus data 

To allow for the variation of the vehicle bus data across different datasets, the `schema_name.bus_data` table may define different columns across different datasets, but must have the same composite *primary key*, as detailed below. The DDL for the table is shown below, where the ellipsis (...) indicate dataset specific columns for storing the vehicle bus data.

	create table schema_name.bus_data
	(
		vehicle_id varchar(255) encode Text255 not NULL,
		scene_id varchar(255) encode Text255 not NULL,
		data_ts BIGINT not NULL sortkey,
		
		... 
	
		primary key(vehicle_id, scene_id, data_ts),
		FOREIGN KEY(vehicle_id) references schema_name.vehicle(vehicleid)
	)
	DISTSTYLE AUTO;

For example, the DDL for the `a2d2.bus_data` table is shown below:

	CREATE TABLE IF NOT EXISTS a2d2.bus_data 
	( 
		vehicle_id varchar(255) encode Text255 not NULL, 
		scene_id varchar(255) encode Text255 not NULL, 
		data_ts BIGINT not NULL sortkey, 
		acceleration_x FLOAT4 not NULL, 
		acceleration_y FLOAT4 not NULL, 
		acceleration_z FLOAT4 not NULL, 
		accelerator_pedal FLOAT4 not NULL, 
		accelerator_pedal_gradient_sign SMALLINT not NULL, 
		angular_velocity_omega_x FLOAT4 not NULL, 
		angular_velocity_omega_y FLOAT4 not NULL, 
		angular_velocity_omega_z FLOAT4 not NULL, 
		brake_pressure FLOAT4 not NULL, 
		distance_pulse_front_left FLOAT4 not NULL, 
		distance_pulse_front_right FLOAT4 not NULL, 
		distance_pulse_rear_left FLOAT4 not NULL, 
		distance_pulse_rear_right FLOAT4 not NULL, 
		latitude_degree FLOAT4 not NULL, 
		latitude_direction SMALLINT not NULL, 
		longitude_degree FLOAT4 not NULL, 
		longitude_direction SMALLINT not NULL, 
		pitch_angle FLOAT4 not NULL, 
		roll_angle FLOAT4 not NULL, 
		steering_angle_calculated FLOAT4 not NULL, 
		steering_angle_calculated_sign SMALLINT not NULL, 
		vehicle_speed FLOAT4 not NULL, 
		primary key(vehicle_id, scene_id, data_ts), 
		FOREIGN KEY(vehicle_id) references a2d2.vehicle(vehicleid) 
	) DISTSTYLE AUTO;

For the A2D2 dataset, the vehicle bus data is automatically loaded into the `a2d2.bus_data` table during the [step-by-step tutorial](#step-by-step-tutorial).

## Runtime services

ADDS runtime services are deployed as [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/pods/) in an [Amazon EKS](https://aws.amazon.com/eks/) cluster. ADDS auto-scales using [Horizontal Pod Autoscaler](https://docs.aws.amazon.com/eks/latest/userguide/horizontal-pod-autoscaler.html), and [Cluster Autoscaler](https://docs.aws.amazon.com/eks/latest/userguide/cluster-autoscaler.html). 

Concretely, there are two manifestations for the ADDS runtime services:

1. Rosbridge data service
2. Kafka data service

### Rosbridge data service 

Rosbridge data service uses [`rosbridge`](http://wiki.ros.org/rosbridge_suite) as the communication channel. The data client connects to the data service via the `rosbridge` web-socket. The data client sends the data request for sensor data on a pre-defined ROS topic, and the data service responds by publishing the requested sensor data on the requested ROS topics. If requested, the data service can serve the response data as ROS bags.

The data client for Rosbridge data service can be any ROS visualization tool that can communicate with `rosbridge`, for example, [Foxglove Studio](https://foxglove.dev/). 

### Kafka data service

Kafka data service uses [Apache Kafka](https://kafka.apache.org/) as the communication channel. The data client sends the data request for sensor data on a pre-defined Kafka topic. The request includes the name of a Kafka response topic. The data service stages the response ROS bag(s) in the ROS bag store and responds with each ROS bag location on the Kafka response topic. 

The data client for Kafka data service is a standalone [Python application](adds/src/kafka_data_client.py) that runs on the desktop and is used in conjunction with [`rviz`](http://wiki.ros.org/rviz) visualization tool. The Python application plays back the response ROS bag files on the local ROS server on the desktop, and the `rviz` tool is used to visualize the playback.  

![High-level system architecture](images/system-arch.jpeg) 

*Figure 1. High-level system architecture for the data service*

The data service runtime uses [Amazon Elastic Kubernetes Service (EKS)](https://aws.amazon.com/eks/).  Raw sensor data store and ROS bag store can be configured to use [Amazon S3](https://aws.amazon.com/s3/), [Amazon FSx for Lustre](https://aws.amazon.com/fsx/), or [Amazon Elastic File System (EFS)](https://aws.amazon.com/efs/). Raw data manifest store uses [Amazon Redshift Serverless](https://aws.amazon.com/redshift/redshift-serverless/). The data processing workflow for building and loading the raw data manifest uses [AWS Batch](https://aws.amazon.com/batch/) with [Amazon Fargate](https://aws.amazon.com/fargate/), [AWS Step Functions](https://aws.amazon.com/step-functions/), and [Amazon Glue](https://aws.amazon.com/glue/). [Amazon Managed Streaming for Apache Kafka (MSK)](https://aws.amazon.com/msk/) provides the communication channel for the Kafka data service. 

The tutorial below walks-through the Rosbridge service, and, optionally, the Kafka service. The Rosbridge service feature set is a super set of the Kafka service.

### Data request for sensor data

Concretely, imagine the data client wants to request drive scene  data from [Audi Autonomous Driving Dataset (A2D2)](https://www.a2d2.audi/a2d2/en.html) for vehicle id `a2d2`, drive scene id `20190401145936`, starting at timestamp `1554121593909500` (microseconds) , and stopping at timestamp `1554122334971448` (microseconds). The data client wants the response to include data **only** from the `front-left camera` in `sensor_msgs/Image` ROS data type, and the `front-left lidar` in `sensor_msgs/PointCloud2` ROS data type. The data client wants the response data to be staged on Amazon FSx for Lustre file system, partitioned across multiple ROS bag files. Such a data request can be encoded in a JSON object, as shown below:

	{
		"vehicle_id": "a2d2",
		"scene_id": "20190401145936",
		"sensor_id": ["lidar/front_left", "camera/front_left"],
		"start_ts": 1554121593909500, 
		"stop_ts": 1554122334971448,
		"ros_topic": {"lidar/front_left": "/a2d2/lidar/front_left", 
				"camera/front_left": "/a2d2/camera/front_left"},
		"data_type": {"lidar/front_left": "sensor_msgs/PointCloud2",
				"camera/front_left": "sensor_msgs/Image"},
		"step": 1000000,
		"accept": "fsx/multipart/rosbag",
		"preview": false,
		...
	}

The `sensor_id` values are keys in `ros_topic` and `data_type` maps that map the sensors to ROS topics, and ROS data types, respectively. For a detailed description of each request field shown in the example above, see [data request fields](#RequestFields).


## Step-by-step tutorial

### Overview

In this tutorial, we use [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html). The high-level outline of the tutorial is as follows:

1. [Prerequisites](#prerequisites)
2. [Configure data service](#configure-data-service)
3. [Build dataset](#build-dataset)
4. [Run Rosbridge data service](#run-rosbridge-data-service)
5. [Run Rosbridge data client](#run-rosbridge-data-client)

You may optionally run Kafka service:

6. [Run Kafka data service](#run-kafka-data-service)
7. [Run Kafka data client](#run-kafka-data-client)

### Prerequisites
This tutorial assumes you have an [AWS Account](https://aws.amazon.com/account/), and you have [system administrator job function](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_job-functions.html) access to the AWS Management Console.

To get started:

* Select your [AWS Region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html). The AWS Regions supported by this project include, us-east-1, us-east-2, us-west-2, eu-west-1, eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, ap-northeast-2, and ap-south-1. The [A2D2](https://registry.opendata.aws/aev-a2d2/) dataset used in this tutorial is stored in  `eu-central-1`.
* If you do not already have an Amazon EC2 key pair, [create a new Amazon EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair). You need the key pair name to specify the `KeyName` parameter when creating the AWS CloudFormation stack below. 
* You need an [Amazon S3](https://aws.amazon.com/s3/) bucket in your selected AWS region. If you don't have one, [create a new Amazon S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) in the selected AWS region. You  use the S3 bucket name to specify the `S3Bucket` parameter in the stack. The bucket is used to store the [A2D2](https://www.a2d2.audi/a2d2/en.html) data.
* Use the [public internet address](http://checkip.amazonaws.com/) of your laptop as the base value for the [CIDR](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) to specify `DesktopRemoteAccessCIDR` parameter in the CloudFormation stack you create below.  
* For all passwords used in this tutorial, we recommend using *strong* passwords using the best-practices recommended for [AWS root account user password](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_passwords_change-root.html).

### Configure data service

#### Create AWS CloudFormation Stack
The [AWS CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html) template `cfn/mozart.yml` in this repository creates [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) resources, so when you [create the CloudFormation Stack using the console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html), in the **Review** step, you must check 
**I acknowledge that AWS CloudFormation might create IAM resources.** 

Create a new AWS CloudFormation stack using the `cfn/mozart.yml` template. The stack input parameters you must specify are described below:

| Parameter Name | Parameter Description |
| --- | ----------- |
| KeyPairName | This is a *required* parameter whereby you select the Amazon EC2 key pair name used for SSH access to the desktop. You must have access to the selected key pair's private key to connect to your desktop. |
| RedshiftMasterUserPassword | This is a *required* parameter whereby you specify the Redshift database master user password.|
| DesktopRemoteAccessCIDR | This is a *required* parameter whereby you specify the public IP CIDR range from where you need remote access to your graphics desktop, e.g. 1.2.3.4/32, or 7.8.0.0/16. |
| DesktopInstanceType | This is a required parameter whereby you select an Amazon EC2 instance type for the ROS desktop. The default value, `g4dn.xlarge`, may not be available for your selected region, in which case, we recommend you try  one of the other available instance types.
| S3Bucket | This is a *required* parameter whereby you specify the name of the Amazon S3 bucket to store your data. **The S3 bucket must already exist.** |

For all other stack input parameters, default values are recommended during first walkthrough. See complete list of all the [template input parameters](#InputParams) below. 

#### Key AWS resources

The key resources in the CloudFormation stack are listed below:

* A ROS desktop EC2 instance (default type `g4dn.xlarge`)
* An Amazon EKS cluster with 2 [managed node groups](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html): `system-nodegroup`, and `work-nodegroup`. Both maanged node groups auto-scale as needed.
* Amazon [Redshift Serverless](https://aws.amazon.com/redshift/redshift-serverless/) workgroup and namespace
* An Amazon [EFS](https://aws.amazon.com/efs/) file system

If you choose to run the optional Kafka data service and client, following additional resources are created:

* An Amazon [MSK](https://aws.amazon.com/msk/) cluster with 3 broker nodes (default type `kafka.m5.large`)
* An Amazon [Fsx for Lustre](https://aws.amazon.com/fsx/lustre/) file system (default size 7,200  GiB)

#### Connect to the graphics desktop using SSH

* Once the stack status in CloudFormation console is `CREATE_COMPLETE`, find the desktop instance launched in your stack in the Amazon EC2 console, and [connect to the instance using SSH](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) as user `ubuntu`, using your SSH key pair.
* When you connect to the desktop using SSH, and you see the message `"Cloud init in progress. Machine will REBOOT after cloud init is complete!!"`, disconnect and try later after about 20 minutes. The desktop installs the NICE DCV server on first-time startup, and reboots after the install is complete.
* If you see the message `NICE DCV server is enabled!`, run the command `sudo passwd ubuntu` to set a new password for user `ubuntu`. Now you are ready to connect to the desktop using the [NICE DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html)

#### Connect to the graphics desktop using NICE DCV Client
* Download and install the [NICE DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html) on your laptop.
* Use the NICE DCV Client to login to the desktop as user `ubuntu`
* When you first login to the desktop using the NICE DCV client, you are asked if you would like to upgrade the OS version. **Do not upgrade the OS version**.

Now you are ready to proceed with the following steps. For all the commands in this tutorial, we assume the *working directory* to be ` ~/amazon-eks-autonomous-driving-data-service` on the graphics desktop. 

#### Configure EKS cluster access

In this step, you will be prompted for [AWS credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) for the **IAM user** you used to create the AWS CloudFormation stack, above. If you instead used an **IAM role** to create the stack, you must first manually [setup the AWS credentials](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html) in the `~/.aws/credentials` file with the following fields:


	[default]
	aws_access_key_id=
	aws_secret_access_key=
	aws_session_token=


The AWS credentials are used one-time to enable EKS cluster access from the ROS desktop, and are *automatically* *removed* at the end of this step. After setting up the credentials, in the *working directory*, run the command:

	./scripts/configure-eks-auth.sh

At the successful execution of this command, you *must* see `AWS Credentials Removed`.

#### Setup developer environment

To setup the developer environment, in the *working directory*, run the command:

	./scripts/setup-dev.sh

This step also builds and pushes the data service container image into [Amazon ECR](https://aws.amazon.com/ecr/).

### Build dataset

In this tutorial, we use [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html) dataset. This dataset is stored in compressed TAR format in `aev-autonomous-driving-dataset` S3 bucket in `eu-central-1`. We need to extract the A2D2 dataset into the S3 bucket for your stack, build the raw data manifest, and load the manifest into the raw data manifest store. To execute these steps, we use an [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html) state machine. To run the AWS Step Functions state machine, run the following command in the *working directory*:

	./scripts/a2d2-etl-steps.sh

The time to complete this step depends on many variable factors, including the choice of your AWS region, and may take anywhere from 12 - 24 hours, or possibly longer.  The AWS Region `eu-central-1` takes the least amount of time for this step because the A2D2 data set is located in this region. 

**Note:** If you have already run `./scripts/a2d2-etl-steps.sh` before in another CloudFormation stack that uses the same Amazon S3 bucket as your current stack, you can complete this step in less than 30 minutes by running the following script, instead:

	./scripts/a2d2-etl-steps-skip-raw.sh
		
Note the `executionArn` of the state machine execution in the output of the previous command. To check the status the status of the execution, use following command, replacing `executionArn` below with your value:

	aws stepfunctions describe-execution --execution-arn executionArn



### Run Rosbridge data service

To deploy the `a2d2-rosbridge` data service, run the following command in the *working directory*:

	helm install --debug a2d2-rosbridge ./a2d2/charts/a2d2-rosbridge/

To verify that the `a2d2-rosbridge` deployment is running, run the command:

	kubectl get pods -n a2d2

This service provides a `Kubernetes service` for data client connection. To find the DNS endpoint for the service, run the command:

	kubectl get svc -n a2d2

The service takes approximately 5 minutes to be ready after it is started, so you may not be able to connect to the service right away.

### Run Rosbridge data client

To publish data requests and visualize the response data, open [Foxglove Studio](https://foxglove.dev/) on the desktop client, and sign-in using your [Foxglove Studio](https://foxglove.dev/) sign-up credentials. Connect Foxglove Studio to your Rosbridge service. In Foxglove Studio, import example layout file [a2d2/config/rosbridge/foxglove/a2d2-ex1.json](a2d2/config/rosbridge/foxglove/a2d2-ex1.json). Publish the data request, and wait for approximately 60 seconds to visualize the response.

The data request is published on the pre-defined ROS topic `/mozart/data_request`. Notice the `accept` field is set to `rosmsg`, which means the data for each requested sensor is directly published on its mapped ROS topic specified in the `ros_topic` map field. More examples can be found under `a2d2/config/rosbridge/foxglove/` folder.

You can exercise *control* on a running data request by publishing ROS messages on the pre-defined ROS topic `/mozart/data_request/control`. For example, to *pause* the request, you can publish:

	{ "data": "{ \"command\": \"pause\" }" }

To *resume* the request, you can publish:

	{ "data": "{ \"command\": \"play\" }" }

To *stop* the request, you can publish:

	{ "data": "{ \"command\": \"stop\" }" }

When you are done with the Rosbridge data service, stop it by executing the command:

	helm uninstall a2d2-rosbridge

### (Optional) Run Kafka data service

Update the CloudFormation stack to set the parameter `DataClientType` to `KafkaAndRosBridge`, and `FsxForLustre` to `enabled`. After CloudFormation update is completed, run the following command in the *working directory*:

	./scripts/setup-dev.sh

To deploy the `a2d2-data-service` Kafka service, run the following command in the *working directory*:

	helm install --debug a2d2-data-service ./a2d2/charts/a2d2-data-service/

To verify that the `a2d2-data-service` deployment is running, run the command:

	kubectl get pods -n a2d2

The data service can be configured to use S3, FSx for Lustre, or EFS (see [Preload A2D2 data from S3 to EFS](#PreloadEFS) ) as the raw sensor data store. The default raw data store is `fsx`, if FSx for Lustre is enabled (see [`FSxForLustre`](#InputParams) parameter), else it is `s3`.

Below is the Helm chart configuration for various raw data store options, with recommended Kubernetes resource requests for pod `memory` and `cpu`. This configuration is used in [`a2d2/charts/a2d2-data-service/values.yaml`](a2d2/charts/a2d2-data-service/values.yaml):

 Data source input | `values.yaml` Configuration |
| --- | ----------- |
| `fsx` (default) | `a2d2.requests.memory: "72Gi"` <br> `a2d2.requests.cpu: "8000m"` <br> `configMap.data_store.input: "fsx"`|
| `efs`  | `a2d2.requests.memory: "32Gi"` <br> `a2d2.requests.cpu: "1000m"` <br> `configMap.data_store.input: "efs"`|
| `s3`  | `a2d2.requests.memory: "8Gi"` <br> `a2d2.requests.cpu: "1000m"` <br> `configMap.data_store.input: "s3"`|

For matching data staging options in data client request, see `request.accept` field in [data request fields](#RequestFields). 

### (Optional) Run Kafka data client

To visualize the response data, we use [rviz2](https://github.com/ros2/rviz) tool on the graphics desktop. Open a terminal on the desktop, and run `rviz2` (`rviz` for ROS 1). 

In the `rviz2` tool, use **File>Open Config** to select  `/home/ubuntu/amazon-eks-autonomous-driving-data-service/a2d2/config/rviz2/a2d2.rviz` as the `rviz` configuration. You should see `rviz2` tool configured with two windows for visualizing response data: image data on the left, and point cloud data on the right. This `rviz2` configuration is specific to the examples we run below.
  
To run the Kafka data client with an example data request, run the following command in the *working directory*:

	python ./a2d2/src/data_client.py --config ./a2d2/config/c-config-ex1.json

After a brief delay, you should be able to *preview* the response data in the `rviz2` tool.

To *preview* data from a different drive scene, execute:

	python ./a2d2/src/data_client.py --config ./a2d2/config/c-config-ex2.json 

 You can set `"preview": false` in the data client config files, and run the above commands again to view the complete response. 

 The data client exits automatically at the end of each successful run. You can use CTRL+C to exit the data client manually.

 When you are done with the Kafka data service, stop it by executing the command:

	helm uninstall a2d2-data-service

### <a name="PreloadEFS"></a> Preload A2D2 data from S3 to EFS 

This step can be executed anytime after [Configure data service](#configure-data-service). and is required **only** if you plan to configure the data service to use EFS as the raw data store, otherwise, it may be safely skipped. Execute following command to start preloading data from your S3 bucket to the EFS file system:

	kubectl apply -n a2d2 -f a2d2/efs/stage-data-a2d2.yaml

To check if the step is complete, execute:

	kubectl get pods stage-efs-a2d2 -n a2d2

If the pod is still `Running`, the step has not yet completed. This step takes approximately 6.5 hours to complete.

## Extending ADDS to other datasets

This section describes how to extend ADDS to work with datasets other than `a2d2`. We recommend reading the entire section before executing any of the steps in this section.

First, we must select a dataset name for the dataset you wish to add to ADDS. The dataset name should start with a letter, be all lowercase, and only contain alphanumeric characters. For the purposes of this documentation, we assume you want to add a dataset named `ds1`. To add a new dataset to ADDS, for example dataset `ds1`, start by executing following command:

	./scripts/add-dataset.sh ds1

The above command copies the [a2d2](a2d2/) folder to `ds1` folder, and customizes the files in the `ds1` folder to the extent automatically possible. This command also copies [a2d2_ros_util.py](adds/src/a2d2_ros_util.py) to `adds/src/ds1_ros_util.py`, ready for your customization. You must complete the customization of the new dataset following the steps below:

1. [Identify vehicle bus data attributes](#identify-vehicle-bus-data-attributes)
2. [Create Redshift schema and tables](#create-redshift-schema-and-tables) 
3. [Load vehicle and sensor data](#load-vehicle-and-sensor-data)
4. [Extract and upload vehicle drive and bus data](#extract-and-load-vehicle-drive-and-bus-data)
5. [Define vehicle bus ROS message](#define-vehicle-bus-ros-message)
6. [Extend `RosUtil`](#extend-rosutil)
7. [Specify calibration data path](#specify-calibration-data-path)
8. [Customize data client configuration files](#customize-data-client-configuration-files)
8. [Apply tutorial steps to your dataset](#apply-tutorial-steps-to-your-dataset)

### Identify vehicle bus data attributes

To extend ADDS to other datasets, you must identify the vehicle bus data attributes for your vehicle fleet. You will need this information to define the [vehicle bus data](#vehicle-bus-data) table columns, and for [defining a new custom ROS message](#define-vehicle-bus-ros-message) for your vehicle bus data. 

### Create Redshift schema and tables

Create a [Redshift schema](https://docs.aws.amazon.com/redshift/latest/dg/r_Schemas_and_tables.html) for your dataset.  Choose the dataset Redshift schema name the same as the dataset name, for example, `ds1`: This is not a hard requirement, but this will make customization for your dataset much simpler.

Create the Redshift tables `ds1.vehicle`, `ds1.sensor`, and `ds1.drive_data`,  using the DDL files in the folder `ds1/ddl/`. 

Recall, when creating the DDL for [vehicle bus data](#vehicle-bus-data) table, you must define the table columns corresponding to the specific attributes in *your vehicle bus data*, while maintaining the *prescribed* primary key. Therefore, modify `ds1/ddl/bus_data.ddl` for your vehicle bus data, and create `ds1.bus_data` table.

### Load vehicle and sensor data

Load data into the `ds1.vehicle` and `ds1.sensor` tables. 

See [vehicle.csv](a2d2/data/vehicle.csv) and [sensors.csv](a2d2/data/sensors.csv) for A2D2 vehicle and sensor data, but note that your data may be be different, and will depend on your vehicle and sensor identifiers. It is recommended that you use user-friendly names to identify the vehicles and sensors, since they appear in each data request.

### Extract and load vehicle drive and bus data

This is the step where you decompose the serialized data acquired in the vehicle into discreet timestamped 2D image and 3D point cloud data frames, and upload the data frames into the ADDS S3 bucket. You must also build a manifest for the data frames, and upload the manifest to the [drive data](#drive-data) table. 

You may want to use an automated workflow to implement this step. In AWS, you have the option of using [AWS Step Functions](https://aws.amazon.com/step-functions/), or [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) for orchestrating the workflow. You may also find [AWS Batch](https://aws.amazon.com/batch/) useful in implementing various steps in the workflow. For example, for the A2D2 dataset, we use AWS Step Functions and AWS Batch to extract and upload the [drive data](#drive-data) and the [vehicle bus data](#vehicle-bus-data). 

You may want to combine the steps [Create Redshift schema and tables](#create-redshift-schema-and-tables), [Load vehicle and sensor data](#load-vehicle-and-sensor-data) and this step in a single script (see, for example, [scripts/a2d2-etl-steps.sh](scripts/a2d2-etl-steps.sh) used for A2D2 dataset).

### Define vehicle bus ROS message

Next, define a [custom ROS 2 message](https://docs.ros.org/en/crystal/Tutorials/Custom-ROS2-Interfaces.html) for encapsulating your vehicle bus data. You may create a [custom ROS 1 message](http://wiki.ros.org/ROS/Tutorials/CreatingMsgAndSrv), if you plan to use ROS 1.

For example, for A2D2 dataset, the custom vehicle bus ROS 2 message, `a2d2_msgs/Bus`, is defined in [adds/colcon_ws/src/a2d2_msgs/](adds/colcon_ws/src/a2d2_msgs/), and for ROS 1 in [adds/catkin_ws/src/a2d2_msgs/](adds/catkin_ws/src/a2d2_msgs/).

Keeping with our assumed dataset name, you could define the custom vehicle bus ROS 2 message in `adds/colcon_ws/src/ds1_msgs/`, and for ROS 1 in `adds/catkin_ws/src/ds1_msgs/`.

### Extend `RosUtil`

In this step, you must extend the [RosUtil](adds/src/common/ros_util.py) [abc](https://docs.python.org/3/library/abc.html) class to implement the *abstract* methods. For example, for the A2D2 dataset, we implement the Python class [a2d2_ros_util.DatasetRosUtil](adds/src/a2d2_ros_util.py) to extend the abstract [RosUtil](adds/src/common/ros_util.py) class.

Keeping our assumed dataset name, modify `adds/src/ds1_ros_util.py` to implement the [RosUtil](adds/src/common/ros_util.py) abstract class for `ds1` dataset. Note, the extended class you implement must be placed under `adds/src`.

### Specify calibration data path

Next, we need to customize the Helm charts used to run ADDS with your dataset. Most of the customization has already been done automatically. You only need to customize  the values in `calibration` fields in `ds1/charts/ds1-data-service/values.yaml` and `ds1/charts/ds1-rosbridge/values.yaml` files. For example, A2D2 dataset uses following `calibration` fields:

	configMap:
	{
		...

		"calibration": {
			"cal_bucket": "",
			"cal_key": "a2d2/cams_lidars.json"
		}
	}

Leave the `calibration.cal_bucket` as shown above.

The `cal_key` must point to the bucket prefix where your vehicle calibration data is stored. The `cal_key` may point to a calibration file, as in the example above, or to an S3 bucket folder: This is dependent on how you store your vehicle calibration data. The python class implemented in [Extend `RosUtil`](#extend-rosutil) uses the calibration data to implement its abstract methods.

### Customize data client configuration files

You must customize the `"requests"` in the data client JSON configuration files under `ds1/config` to work with your dataset. We will explain this customization using the example of [c-config-ex1.json](a2d2/config/c-config-ex1.json), which makes a data request for `a2d2` data:

	"requests": [{
		"kafka_topic": "a2d2", 
		"vehicle_id": "a2d2",
		"scene_id": "20190401121727",
		"sensor_id": ["bus", "lidar/front_left", "camera/front_left"],
		"start_ts": 1554115465612291, 
		"stop_ts": 1554115765612291,
		"ros_topic": {"bus": "/a2d2/bus", "lidar/front_left": "/a2d2/lidar/front_left", 
				"camera/front_left": "/a2d2/camera/front_left"},
		"data_type": {"bus": "a2d2_msgs/Bus", "lidar/front_left": "sensor_msgs/PointCloud2",
				"camera/front_left": "sensor_msgs/Image"},
		"step": 1000000,
		"accept": "fsx/multipart/rosbag",
		"preview": true
	}]

All the fields above need to be customized for your dataset. For example, your `kafka_topic` value will be `ds1`. Your `vehicle_id` will depend on the values in the `ds1.vehicle` table. your `sensor_id` values (except for the implicit value `bus`, which is always the same for all datasets) will depend on the values in the `ds1.sensor`  table. The Ros message data type for your bus data will be different. Your `scene_id` will be different. Your `start_ts` and `stop_ts` values will be different. Your `ros_topic` values may be different. You will need to customize all these values so you can request data from `ds1` dataset.

### Apply tutorial steps to your dataset

Next, walk-through the [step-by-step tutorial](#step-by-step-tutorial), but starting with the step [Setup developer environment](#setup-developer-environment). You will need to make following changes to the tutorial steps, so you can use ADDS with `ds1` dataset:

* Instead of `a2d2`, use `ds1`. 
* In [Build dataset](#build-dataset) step, you will need to  execute your ETL script instead of [scripts/a2d2-etl-steps.sh](scripts/a2d2-etl-steps.sh) so you can launch your workflow to upload your data into S3 and Redshift tables.

## Deleting the AWS CloudFormation stack

When you no longer need the ADDS data service, you may delete the AWS CloudFormation stack from the AWS CloudFormation console. Deleting the CloudFormation stack deletes all the resources in the stack (including FSx for Lustre and EFS), *except for the Amazon S3 bucket*.

## <a name="Reference"></a> Reference

### <a name="RequestFields"></a> Data client request fields
Below, we explain the semantics of the various fields in the data client request JSON object.

| Request field name | Request field description |
| --- | ----------- |
| `servers` | The `servers` identify the [AWS MSK](https://aws.amazon.com/msk/) Kafka cluster endpoint. |
| `delay` | The `delay` specifies the delay in seconds that the data client delays sending the request. Default value is `0`. |
| `use_time` | (Optional) The `use_time` specifies whether to use the `received`  time, or `header` time when playing back the received messages. Default value is `received`. |
| `requests`| The JSON document sent by the client to the data service must include an array of one or more data `request` objects. |
| `request.kafka_topic` | For Kafka data service only. The `kafka_topic` specifies the Kafka topic on which the data request is sent from the client to the Kafka data service. |
| `request.vehicle_id` | The `vehicle_id` is used to identify the relevant drive scene dataset. |
| `request.scene_id`  | The `scene_id` identifies the drive scene of interest, which in this example is `20190401145936`, which in this example is a string representing the date and time of the drive scene, but in general could be any unique value. |
| `request.start_ts` | The `start_ts` (microseconds) specifies the start timestamp for the drive scene data request. |
| `request.stop_ts` | The `stop_ts` (microseconds) specifies the stop timestamp for the drive scene data request. |
| `request.ros_topic` | The `ros_topic` is a map from `sensor ids` in the vehicle to ROS topics.|
| `request.data_type`| The `data_type` is a map from `sensor ids` to ROS data types.  |
| `request.step` | The `step` is the discreet time interval (microseconds) used to discretize the timespan between `start_ts` and `stop_ts`. If `request.accept` value contains `multipart`, the data service responds with a  ROS bag for each discreet `step`: See [possible values](#AcceptValues) below. |
| `request.accept` | The `accept` specifies the response data staging format acceptable to the client: See [possible values](#AcceptValues) below. |
| `request.image` | (Optional) The value `undistorted` undistorts the camera image. Undistoring an image slows down the image frame rate. Default value is `original` distorted image.|
| `request.lidar_view` | (Optional) The value `vehicle` transforms lidar points to `vehicle` frame of reference view. Default value is `camera`.|
|`request.preview`| If the `preview` field is set to `true`, the data service returns requested data over a single time `step` starting from `start_ts` , and ignores the `stop_ts`.|
|`request.no_playback`| (Optional) This only applies to Kafka data client. If the `no_playback` field is set to `true`, the data client *does not playback* the response ROS bags. Default value is `false`.|
|`request.storage_id`| For ROS2 only. The storage id of `rosbag2` storage plugin. The default value is `mcap`. (See [rosbag2_storage_mcap](https://github.com/ros-tooling/rosbag2_storage_mcap) )|
|`request.storage_preset_profile`| For ROS2 only. The storage preset profile of `rosbag2` storage plugin. The default value is `zstd_fast`. (See [rosbag2_storage_mcap](https://github.com/ros-tooling/rosbag2_storage_mcap) )|

#### <a name="AcceptValues"></a>  Possible `request.accept` field values

Rosbridge data service publishes response data on ROS topic `/mozart/data_response` for all values of `request.accept` shown below, except `rosmsg`. 

| `request.accept` value| Description |
| ---------------------------| ----------- |
| `rosmsg`               | For Rosbridge data service only. Publish response data on the requested ROS topics.|
| `fsx/multipart/rosbag` | Stage response data on Amazon FSx for Lustre in multiple ROS bags. |
| `efs/multipart/rosbag` | Stage response data on Amazon EFS in multiple ROS bags. |
|`s3/multipart/rosbag`   | Stage response data on Amazon S3 in multiple ROS bags. |
| `fsx/singlepart/rosbag`| Stage response data on Amazon FSx for Lustre in a single ROS bag. |
| `efs/singlepart/rosbag`| Stage response data on Amazon EFS in a single ROS bag. |
|`s3/singlepart/rosbag`  | Stage response data on Amazon S3 in a single ROS bag. |
| `manifest`             | Respond with a manifest of S3 paths to raw data. |

### <a name="InputParams"></a> AWS CloudFormation template input parameters
This repository provides an [AWS CloudFormation](https://aws.amazon.com/cloudformation/) template that is used to create the required stack.

Below, we describe the AWS CloudFormation [template](cfn/mozart.yml) input parameters. Desktop below refers to the NICE DCV enabled high-performance graphics desktop that acts as the data service client in this tutorial.

| Parameter Name | Parameter Description |
| --- | ----------- |
| DesktopInstanceType | This is a **required** parameter whereby you select an Amazon EC2 instance type for the desktop running in AWS cloud. Default value is `g4dn.xlarge`. |
| DesktopEbsVolumeSize | This is a **required** parameter whereby you specify the size of the root EBS volume (default size is 200 GB) on the desktop. Typically, the default size is sufficient.|
| DesktopEbsVolumeType | This is a **required** parameter whereby you select the [EBS volume type](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html) (default is gp3). |
| DesktopHasPublicIpAddress | This is a **required** parameter whereby you select whether a Public Ip Address be associated with the Desktop.  Default value is `true`.|
| DesktopRemoteAccessCIDR | This parameter specifies the public IP CIDR range from where you need remote access to your client desktop, e.g. 1.2.3.4/32, or 7.8.0.0/16. |
| DesktopType | This parameter specifies support for `Graphical` desktop with NICE-DCV server enabled, or `Headless` desktop with NICE-DCV server disabled. Default value is `Graphical`.|
| DataClientType | This parameter specifies support for `RosBridge`, and `KafkaAndRosBridge`. Default value is `RosBridge`.|
| EKSEncryptSecrets | This is a **required** parameter whereby you select if encryption of EKS secrets is `Enabled`. Default value is `Enabled`.|
| EKSEncryptSecretsKmsKeyArn | This is an *optional* advanced parameter whereby you specify the [AWS KMS](https://aws.amazon.com/kms/) key ARN that is used to encrypt EKS secrets. Leave blank to create a new KMS key.|
| EKSNodeVolumeSizeGiB | This is a **required** parameter whereby you specify EKS Node group instance EBS volume size. Default value is 200 GiB.|
| EKSSystemNodeGroupCapacityType | This is a **required** parameter whereby you specify EKS system node group capacity type: `SPOT`, or `ON_DEMAND`. Default value is `SPOT`|
| EKSSystemNodeGroupInstanceType | This is a **required** parameter whereby you specify EKS system node group instance types as a comma separated list. Default value is `"t3a.small,t3a.medium,t3a.large,m5a.large,m7a.large"`|
| EKSWorkNodeGroupCapacityType | This is a **required** parameter whereby you specify EKS work node group capacity type: `SPOT`, or `ON_DEMAND`. Default value is `SPOT`|
| EKSWorkNodeGroupInstanceType | This is a **required** parameter whereby you specify EKS work node group instance types as a comma separated list. Default value is `"m5a.8xlarge,m5.8xlarge,m5n.8xlarge,m7a.8xlarge,r5n.8xlarge"`|
| EKSWorkNodeGroupMaxSize | This is a **required** parameter whereby you specify EKS work node group maximum size. Default value is 16 nodes. Cluster auto-scaler scales this node group as needed.|
| FargateComputeType | This is a **required** parameter whereby you specify Fargate compute environment type. Allowed values are `FARGATE_SPOT` and `FARGATE`. Default value is `FARGATE_SPOT`. |
| FargateComputeMax | This is a **required** parameter whereby you specify maximum size of Fargate compute environment in vCpus. Default value is `1024`.|
| FSxForLustre |  This is a **required** parameter whereby you specify whether FSx for Lustre is `enabled`, or `disabled`. Default value is `disabled`.|
| FSxStorageCapacityGiB |  This is a **required** parameter whereby you specify the FSx Storage capacity, which must be in multiples of `2400 GiB`. Default value is `7200 GiB`.|
| FSxS3ImportPrefix | This is an *optional* advanced parameter whereby you specify FSx S3 bucket path prefix for importing data from S3 bucket. Leave blank to import the complete bucket.|
| KeyPairName | This is a **required** parameter whereby you select the Amazon EC2 key pair name used for SSH access to the desktop. You must have access to the selected key pair's private key to connect to your desktop. |
| KubectlVersion | This is a **required** parameter whereby you specify EKS `kubectl` version. Default value is `1.28.3/2023-11-14`. |
| KubernetesVersion | This is a **required** parameter whereby you specify EKS cluster version. Default value is `1.28`. |
| MSKBrokerNodeType | This is a **required** parameter whereby you specify the type of node to be provisioned for AWS MSK Broker. |
| MSKNumberOfNodes | This is a **required** parameter whereby you specify the number of MSK Broker nodes, which must be >= 2. |
| PrivateSubnet1CIDR | This is a **required** parameter whereby you specify the Private Subnet1 CIDR in Vpc CIDR. Default value is `172.30.64.0/18`.|
| PrivateSubnet2CIDR | This is a **required** parameter whereby you specify the Private Subnet2 CIDR in Vpc CIDR. Default value is `172.30.128.0/18`.|
| PrivateSubnet3CIDR | This is a **required** parameter whereby you specify the Private Subnet3 CIDR in Vpc CIDR. Default value is `172.30.192.0/18`.|
| PublicSubnet1CIDR | This is a **required** parameter whereby you specify the Public Subnet1 CIDR  in Vpc CIDR. Default value is `172.30.0.0/24`.|
| PublicSubnet2CIDR | This is a **required** parameter whereby you specify the Public Subnet2 CIDR  in Vpc CIDR. Default value is `172.30.1.0/24`.|
| PublicSubnet3CIDR | This is a **required** parameter whereby you specify the Public Subnet3 CIDR  in Vpc CIDR. Default value is `172.30.2.0/24`.|
| RedshiftNamespace | This is a **required** parameter whereby you specify the Redshift Serverless namespace. Default value is `mozart`.|
| RedshiftWorkgroup | This is a **required** parameter whereby you specify the Redshift Serverless workgroup. Default value is `mozart`.|
| RedshiftServerlessBaseCapacity | This is a **required** parameter whereby you specify the Redshift Serverless base capacity in DPUs. Default value is `128`.|
| RedshiftDatabaseName | This is a **required** parameter whereby you specify the name of the Redshift database. Default value is `mozart`.|
| RedshiftMasterUsername | This is a **required** parameter whereby you specify the name Redshift Master user name. Default value is `admin`.|
| RedshiftMasterUserPassword | This is a **required** parameter whereby you specify the name Redshift Master user password.|
| RosVersion | This is a **required** parameter whereby you specify the version of [ROS](https://ros.org/). The supported versions are `melodic` on Ubuntu Bionic,  `noetic`  on Ubuntu Focal, and `humble` on Ubuntu Jammy. Default value is `humble`.|
| S3Bucket | This is a **required** parameter whereby you specify the name of the Amazon S3 bucket to store your data. |
| UbuntuAMI | This is an *optional* advanced parameter whereby you specify Ubuntu AMI (18.04 or 20.04).|
| VpcCIDR | This is a **required** parameter whereby you specify the [Amazon VPC](https://aws.amazon.com/vpc/?vpc-blogs.sort-by=item.additionalFields.createdDate&vpc-blogs.sort-order=desc) CIDR for the VPC created in the stack. Default value is 172.30.0.0/16. If you change this value, all the subnet parameters above may need to be set, as well.|