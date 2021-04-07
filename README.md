# Data Service for ADAS and ADS Development

## Overview

This is an example of a data service typically used in advanced driver assistance systems (ADAS), and automated driving systems (ADS) development. 

The typical use case addressed by this data service is to serve a single [```rosbag```](http://wiki.ros.org/rosbag)  file, or a series of ```rosbag``` files containing data from a drive scene of interest. In case of the ```rosbag``` series, each ```rosbag``` file contains the drive scene data for a single time step (default time step is 1s). Each ```rosbag``` file is composed from the drive scene data stored in [Amazon S3](https://aws.amazon.com/free/storage/s3), using the meta-data stored in [Amazon Redshift](https://aws.amazon.com/redshift/). 


## Key concepts

The data service runs in [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/) in an [Amazon EKS](https://aws.amazon.com/eks/) cluster. An [Amazon Managed Service For Apache Kafka](https://aws.amazon.com/msk/) (MSK) cluster provides the communication channel between the data client, and the data service. The data service implements a *request-response* paradigm over Kafka topics. However, the response data is not sent back over the Kafka topics. Instead, the data service stages the response data in Amazon S3, Amazon FSx, or Amazon EFS, as specified in the data service configuration.

### Data client request
Concretely, imagine the data client wants to request drive scene  data in ```rosbag``` file format from [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html) for drive scene id ```20190401145936```, starting at timestamp ```1554121593909500``` (microseconds) , and stopping at timestamp ```1554122334971448``` (microseconds). 

The data client wants the response to include data **only** from the ```front-left camera``` in ```sensor_msgs/Image``` [ROS](https://www.ros.org/) data type, and the ```front-left lidar``` in ```sensor_msgs/PointCloud2``` ROS data type. Further, imagine the data client wants the response data to be chunked in series of ```rosbag``` files, each spanning ```1000000``` microseconds. Finally, the data client wants the response ```rosbag``` files to be stored on a shared Amazon FSx file system. 

The data client can encode such a data request using the JSON object shown below, and send it to the (imaginary) Kafka cluster endpoint ```b-1.msk-cluster-1:9092,b-2.msk-cluster-1:9092``` on the Kafka topic ```a2d2```:

```
{
	"servers": "b-1.msk-cluster-1:9092,b-2.msk-cluster-1:9092",
	"requests": [{
		"kafka_topic": "a2d2", 
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
		"preview": false
	}]
}
```

For a detailed description of each request field shown in the example above, see [data request fields](#RequestFields) below.

### Raw data input data source, and response data staging

The data service can be configured to input raw data from S3, FSx (see [Preload A2D2 data from S3 to FSx](#PreloadFSx) ), or EFS (see [Preload A2D2 data from S3 to EFS](#PreloadEFS) ).  Similarly, the data client can specify the ```accept``` field in the ```request``` to request that the response data be staged on S3, FSx, or EFS. Any combination of raw data input source, and the response data staging is valid. 

The raw data input source is fixed when the data service is deployed. However, the response data staging option is specified in the data client request, and is therefore dynamic. 

## Tutorial step by step guide

### Overview
In this tutorial, we use [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html). The high-level outline of this tutorial is as follows:

* Prerequisites
* Create [AWS CloudFormation](https://aws.amazon.com/cloudformation/) stack
* ETL metadata from the raw data in the [Amazon S3](https://aws.amazon.com/s3/) bucket into the [Amazon Redshift](https://aws.amazon.com/redshift/) database
* Deploy the data service in the [Amazon EKS](https://aws.amazon.com/eks/) cluster
* Send a request to the data service from the data client, and visualize the response


### Prerequisites
This tutorial assumes you have an [AWS Account](https://aws.amazon.com/account/), and you have [Administrator job function](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_job-functions.html) access to the AWS Management Console.

To get started:

* Select your [AWS Region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html). The AWS Regions supported by this project include, us-east-1, us-east-2, us-west-2, eu-west-1, eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, ap-northeast-2, and ap-south-1. Note that not all Amazon EC2 instance types are available in all [AWS Availability Zones](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) in an AWS Region.
* Subscribe to [Ubuntu Pro 18.04 LTS](https://aws.amazon.com/marketplace/pp/Amazon-Web-Services-Ubuntu-Pro-1804-LTS/B0821T9RL2) and [Ubuntu Pro 20.04 LTS](https://aws.amazon.com/marketplace/pp/Amazon-Web-Services-Ubuntu-Pro-2004-LTS/B087L1R4G4).
* If you do not already have an Amazon EC2 key pair, [create a new Amazon EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair). You will need the key pair name to specify the ```KeyName``` parameter when creating the AWS CloudFormation stack below. 
* You will need an [Amazon S3](https://aws.amazon.com/s3/) bucket. If you don't have one, [create a new Amazon S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html) in the AWS region of your choice. You will use the S3 bucket name to specify the ```S3Bucket``` parameter in the stack. 
* Run ```curl ifconfig.co``` on your laptop and note your public IP address. This will be the IP address you will need to specify ```DesktopAccessCIDR``` parameter in the stack.


### Create AWS CloudFormation Stack
The [AWS CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html) template ```cfn/mozart.yml``` in this repository creates [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) resources, so when you [create the CloudFormation Stack using the console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html), you must check 
**I acknowledge that AWS CloudFormation might create IAM resources.** 

Create a new AWS CloudFormation stack using the ```cfn/mozart.yml``` template. The stack input parameters you must specify are described below:

| Parameter Name | Parameter Description |
| --- | ----------- |
| KeyPairName | This is a *required* parameter whereby you select the Amazon EC2 key pair name used for SSH access to the desktop. You must have access to the selected key pair's private key to connect to your desktop. |
| RedshiftMasterUserPassword | This is a *required* parameter whereby you specify the Redshift database master user password.|
| RemoteAccessCIDR | This is a *required* parameter whereby you specify the public IP CIDR range from where you need remote access to your graphics desktop, e.g. 1.2.3.4/32, or 7.8.0.0/16. |
| S3Bucket | This is a *required* parameter whereby you specify the name of the Amazon S3 bucket to store your data. **The S3 bucket must already exist.** |

For all other stack input parameters, default values are recommended during first walkthrough. See complete list of all the [template input parameters](#InputParams) below. See the stack outputs in CloudFormation console to view all the resources created in the stack. 

The key resources created in the stack include an [Amazon EKS](https://aws.amazon.com/eks/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) cluster, an [AWS MSK](https://aws.amazon.com/msk/) cluster, an [Amazon Redshift](https://aws.amazon.com/redshift/) cluster, and an [Amazon EC2](https://aws.amazon.com/ec2/?ec2-whats-new.sort-by=item.additionalFields.postDateTime&ec2-whats-new.sort-order=desc) graphics desktop.  

During stack creation, this repository is automatically cloned on the graphics desktop under ```/home/ubuntu```. Relevant configuration files are automatically generated and placed under ```a2d2/config``` directory. Required environment variables are appended to ```/home/ubuntu/.bashrc``` file. Applicable EKS YAML files under ```scripts```, and [Helm Values](https://helm.sh/docs/chart_template_guide/values_files/) files  under ```a2d2/charts``` are also automatically updated with the correct values from the stack outputs. 

## Connect to the graphics desktop using SSH

* Once the stack status in CloudFormation console is ```CREATE_COMPLETE```, find the deep learning desktop instance launched in your stack in the Amazon EC2 console, and [connect to the instance using SSH](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) as user ```ubuntu```, using your SSH key pair.
* When you connect to the desktop using SSH, and you see the message ```"Cloud init in progress. Machine will REBOOT after cloud init is complete!!"```, disconnect and try later after about 20 minutes. The desktop installs the NICE DCV server on first-time startup, and reboots after the install is complete.
* If you see the message ```NICE DCV server is enabled!```, run the command ```sudo passwd ubuntu``` to set a new password for user ```ubuntu```. Now you are ready to connect to the desktop using the [NICE DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html)

### Connect to the graphics desktop using NICE DCV Client
* Download and install the [NICE DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html) on your laptop.
* Use the NICE DCV Client to login to the desktop as user ```ubuntu```
* When you first login to the desktop using the NICE DCV client, you will be asked if you would like to upgrade the OS version. **Do not upgrade the OS version** .

Now you are ready to proceed to the following steps.

### Set working directory

For all the commands in this tutorial, we assume the working directory to be the root of this git repository cloned on the graphics desktop. Open a terminal, and execute the following command to set the *working directory*:

	cd ~/amazon-eks-autonomous-driving-data-service 

### Configure the data service infrastructure

For this step, you need your [AWS credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) with Administrator job function programmatic access. The AWS credentials are only used for this step and automatically removed at the end of this step. This step executes following actions:

* Configure ```kubectl``` to use the [EC2 instance profile](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html) role
* Create ```a2d2``` Kubernetes namespace 
* Create an IAM OIDC provider for the EKS cluster
* Deploy Amazon FSx for Luster CSI driver, and create EKS persistent volume
* Deploy Amazon EFS CSI driver, and create EKS persistent volume
* Create IAM role for EKS pods
* Update the MSK cluster configuration to enable automatic Kafka topics

In the *working directory*, run the command:

		./scripts/setup-dev.sh

At the end of this command output, you should see ```AWS Credentials Removed```. Verify that there is no ```~/.aws/credentials``` file on the graphics desktop.

### Extract A2D2 dataset to your S3 bucket

To extract the compressed public [A2D2 dataset](https://registry.opendata.aws/aev-a2d2/) from ```aev-autonomous-driving-dataset``` S3 bucket to your S3 bucket under ```a2d2``` prefix, execute the following command in the *working directory*:

		nohup ./scripts/copy-a2d2.sh 1>/tmp/copy-a2d2.log 2>&1 &

You need to wait for this script command to be completed before proceeding with the following steps. **It may take up to 24 hours to extract the A2D2 dataset into your S3 bucket.** 

### ETL metadata to Redshift

Once the previous step is complete, you are ready to extract the metadata from the raw data, and load it into the Redshift database: We do this in two steps. 

#### ETL Step 1: Extract metadata from the raw A2D2 data 
To extract the metadata from the raw A2D2 data, and store it in your S3 bucket, run an [AWS Glue](https://aws.amazon.com/glue/) ETL job by executing the following command in the *working directory*:

		python3 ./scripts/glue-etl-job.py --config ./a2d2/config/glue.config

The ```./a2d2/config/glue.config``` file is automatically generated when your created the stack: It contains the required parameters to create the Glue job. In the output, you should see the job status. 

If for any reason you have to rerun this step, first delete all the data in the ```emr``` prefix in your S3 bucket, and then rerun the step.

#### ETL Step 2: Load metadata from S3 to Redshift

To load the metadata into the Redshift database, edit ```./a2d2/config/redshift.config``` to specify the Redshift database password in the ```password``` field, and execute the following command in the *working directory*:

		python3 ./scripts/setup-redshift-db.py --config ./a2d2/config/redshift.config  

The ```./a2d2/config/redshift.config``` file is automatically generated when you created the stack: It contains Redshift connection information, and the queries for creating the schema, the tables, and uploading the metadata from S3 to Redshift.

In the output you should see the Redshift queries being executed.


### Deploy the data service in EKS using Helm Charts

*For best performance, [preload A2D2 data from S3 to FSx](#PreloadFSx).*

The data service is deployed using an [Helm Chart](https://helm.sh/docs/topics/charts/), and runs as a ```kubernetes deployment``` in EKS. To build and push the required Docker image to [Amazon ECR](https://aws.amazon.com/ecr/), execute the following command in the *working directory*:

		./scripts/build-ecr-image.sh

This step automatically updates the ```a2d2/charts/a2d2-data-service/values.yaml```  and ```a2d2/charts/a2d2-rosbridge/values.yaml``` files with the ECR image URI.

Edit ```a2d2/charts/a2d2-data-service/values.yaml``` file, and set  ```configMap.database.password``` to your Redshift database password: All other values have been automatically updated.

To start the A2D2 data service, execute the following command in the *working directory*:

		helm install --debug a2d2-data-service ./a2d2/charts/a2d2-data-service/

To verify that the ```a2d2-data-service``` deployment is running, execute the command:

		kubectl get pods -n a2d2
		
You can stop the running data service by executing:

		helm delete a2d2-data-service
 

### Run data client


To visualize the response data requested by the A2D2 data client, we will use [rviz](http://wiki.ros.org/rviz) tool on the graphics desktop. Open a terminal on the desktop, and run ```rviz```. In the ```rviz``` tool, set ```/home/ubuntu/amazon-eks-autonomous-driving-data-service/a2d2/config/a2d2.rviz``` as the ```rviz``` config file. You should see ```rviz``` tool now configured with two areas, one for visualizing image data, and the other for visualizing point cloud data.
  
To run the data client, execute the following command in the *working directory*:

		python ./a2d2/src/data_client.py \
			--config ./a2d2/config/c-config-ex1.json 1>/tmp/a.out 2>&1 & 

After a brief delay, you should be able to preview the response data in the ```rviz``` tool To preview data from a different drive scene, execute:

		python ./a2d2/src/data_client.py \
			--config ./a2d2/config/c-config-ex2.json 1>/tmp/a.out 2>&1 & 


You can set ```"preview": false``` in the data client config file, and run the above command to view the complete response.
			
### Killing data service client

Killing the data client does not stop the data service from producing and sending the ```rosbag``` files to the client, as the data information is being sent asynchronously. If the data client is killed by the user, the data service will continue to stage the response data, and the data will remain stored on the response data staging option specified in the data client request.

If you kill the data service client before it exits normally, be sure to clean up all the processes spawned by the data client. 


## <a name="Reference"></a> Reference

### <a name="RequestFields"></a> Data client request fields
Below, we explain the semantics of the various fields in the data client request JSON object.

| Request field name | Request field description |
| --- | ----------- |
| ```servers``` | The ```servers``` identify the [AWS MSK](https://aws.amazon.com/msk/) Kafka cluster endpoint. |
| ```requests```| The JSON document sent by the client to the data service must include an array of one or more data ```requests``` for drive scene data. |
| ```kafka_topic``` | The ```kafka_topic``` specifies the Kafka topic on which the data request is sent from the client to the data service. The data service is listening on the topic. |
| ```vehicle_id``` | The ```vehicle_id``` is used to identify the relevant drive scene dataset. |
| ```scene_id```  | The ```scene_id``` identifies the drive scene of interest, which in this example is ```20190401145936```, which in this example is a string representing the date and time of the drive scene, but in general could be any unique value. |
| ```start_ts``` | The ```start_ts``` (microseconds) specifies the start timestamp for the drive scene data request. |
| ```stop_ts``` | The ```stop_ts``` (microseconds) specifies the stop timestamp for the drive scene data request. |
| ```ros_topic``` | The ```ros_topic``` is a dictionary from ```sensor ids``` in the vehicle to ```ros``` topics.|
| ```data_type```| The ```data_type``` is a dictionary from ```sensor ids``` to ```ros``` data types.  |
| ```step``` | The ```step``` is the discreet time interval (microseconds) used to discretize the timespan between ```start_ts``` and ```stop_ts``` into discreet chunks. The data service responds with a ```rosbag``` file for each chunk, if ```accept``` field contains ```multipart``` . See also ```singlepart``` field.|
| ```accept``` | The ```accept``` specifies the response data staging format acceptable to the client. |
| ```fsx/multipart/rosbag``` | Format for response data staging on [Amazon FSx for Lustre](https://aws.amazon.com/fsx/lustre/) as  discretized ```rosbag``` chunks. |
| ```efs/multipart/rosbag``` | Format for response data staging on [Amazon EFS](https://aws.amazon.com/efs/) as  discretized ```rosbag``` chunks.|
|```s3/multipart/rosbag```| Format for response data staging on [Amazon S3](https://aws.amazon.com/s3/) as  discretized ```rosbag``` chunks.|
| ```singlepart```| If the ```accept``` specifies ```singlepart```, the response data comprises of a single ```rosbag``` file, instead of the discretized ```rosbag``` chunks in the case of ```multipart```. See also ```step``` field. |
| ```manifest``` | If the ```accept``` is specified as ```manifest```, meta-data containing S3 paths to the raw data is returned, and the data client is expected to process the meta-data as it deems fit. |
|```preview```| If the ```preview``` field is set to ```true```, the data service returns requested data over a single time ```step``` starting from ```start_ts``` , but ignores the ```stop_ts```.|

### <a name="InputParams"></a> AWS CloudFormation template input parameters
This repository provides an [AWS CloudFormation](https://aws.amazon.com/cloudformation/) template that is used to create the required stack.

Below, we describe the AWS CloudFormation [template](cfn/mozart.yml) input parameters. Desktop below refers to the NICE DCV enabled high-performance graphics desktop that acts as the data service client in this tutorial.

| Parameter Name | Parameter Description |
| --- | ----------- |
| DesktopInstanceType | This is a **required** parameter whereby you select an Amazon EC2 instance type for the desktop running in AWS cloud. Default value is ```g3s.xlarge```. |
| DesktopEbsVolumeSize | This is a **required** parameter whereby you specify the size of the root EBS volume (default size is 200 GB) on the desktop. Typically, the default size is sufficient.|
| DesktopEbsVolumeType | This is a **required** parameter whereby you select the [EBS volume type](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html) (default is gp3). |
| DesktopHasPublicIpAddress | This is a **required** parameter whereby you select whether a Public Ip Address be associated with the Desktop.  Default value is ```true```.|
| EKSEncryptSecrets | This is a **required** parameter whereby you select if encryption of EKS secrets is ```Enabled```. Default value is ```Enabled```.|
| EKSEncryptSecretsKmsKeyArn | This is an *optional* advanced parameter whereby you specify the [AWS KMS](https://aws.amazon.com/kms/) key ARN that is used to encrypt EKS secrets. Leave blank to create a new KMS key.|
| EKSNodeGroupInstanceType | This is a **required** parameter whereby you select EKS Node group EC2 instance type. Default value is ```r5n.8xlarge```.|
| EKSNodeVolumeSizeGiB | This is a **required** parameter whereby you specify EKS Node group instance EBS volume size. Default value is 200 GiB.|
| EKSNodeGroupMinSize | This is a **required** parameter whereby you specify EKS Node group minimum size. Default value is 1 node.|
| EKSNodeGroupMaxSize | This is a **required** parameter whereby you specify EKS Node group maximum size. Default value is 8 nodes.|
| EKSNodeGroupDesiredSize | This is a **required** parameter whereby you specify EKS Node group initial desired size. Default value is 2 nodes.|
| FSxStorageCapacityGiB |  This is a **required** parameter whereby you specify the FSx Storage capacity, which must be in multiples of 3600 GiB. Default value is 7200 GiB.|
| FSxS3ImportPrefix | This is an *optional* advanced parameter whereby you specify FSx S3 bucket path prefix for importing data from S3 bucket. Leave blank to import the complete bucket.|
| KeyPairName | This is a **required** parameter whereby you select the Amazon EC2 key pair name used for SSH access to the desktop. You must have access to the selected key pair's private key to connect to your desktop. |
| KubectlVersion | This is a **required** parameter whereby you specify EKS ```kubectl``` version. Default value is ```1.19.6/2021-01-05```. |
| KubernetesVersion | This is a **required** parameter whereby you specify EKS cluster version. Default value is ```1.19```. |
| MSKBrokerNodeType | This is a **required** parameter whereby you specify the type of node to be provisioned for AWS MSK Broker. |
| MSKNumberOfNodes | This is a **required** parameter whereby you specify the number of MSK Broker nodes, which must be >= 2. |
| PrivateSubnet1CIDR | This is a **required** parameter whereby you specify the Private Subnet1 CIDR in Vpc CIDR. Default value is ```172.30.64.0/18```.|
| PrivateSubnet2CIDR | This is a **required** parameter whereby you specify the Private Subnet2 CIDR in Vpc CIDR. Default value is ```172.30.128.0/18```.|
| PrivateSubnet3CIDR | This is a **required** parameter whereby you specify the Private Subnet3 CIDR in Vpc CIDR. Default value is ```172.30.192.0/18```.|
| PublicSubnet1CIDR | This is a **required** parameter whereby you specify the Public Subnet1 CIDR  in Vpc CIDR. Default value is ```172.30.0.0/24```.|
| PublicSubnet2CIDR | This is a **required** parameter whereby you specify the Public Subnet2 CIDR  in Vpc CIDR. Default value is ```172.30.1.0/24```.|
| PublicSubnet3CIDR | This is a **required** parameter whereby you specify the Public Subnet3 CIDR  in Vpc CIDR. Default value is ```172.30.2.0/24```.|
| RedshiftDatabaseName | This is a **required** parameter whereby you specify the name of the Redshift database. Default value is ```mozart```.|
| RedshiftMasterUsername | This is a **required** parameter whereby you specify the name Redshift Master user name. Default value is ```admin```.|
| RedshiftMasterUserPassword | This is a **required** parameter whereby you specify the name Redshift Master user password.|
| RedshiftNodeType | This is a **required** parameter whereby you specify the type of node to be provisioned for Redshift cluster. Default value is ```ra3.4xlarge```.|
| RedshiftNumberOfNodes | This is a **required** parameter whereby you specify the number of compute nodes in the Redshift cluster, which must be >= 2.|
| RedshiftPortNumber | This is a **required** parameter whereby you specify the port number on which the Redshift cluster accepts incoming connections. Default value is ```5439```.|
| RemoteAccessCIDR | This parameter specifies the public IP CIDR range from where you need remote access to your client desktop, e.g. 1.2.3.4/32, or 7.8.0.0/16. |
| S3Bucket | This is a **required** parameter whereby you specify the name of the Amazon S3 bucket to store your data. |
| UbuntuAMI | This is an *optional* advanced parameter whereby you specify Ubuntu AMI (18.04 or 20.04).|
| VpcCIDR | This is a **required** parameter whereby you specify the [Amazon VPC](https://aws.amazon.com/vpc/?vpc-blogs.sort-by=item.additionalFields.createdDate&vpc-blogs.sort-order=desc) CIDR for the VPC created in the stack. Default value is 172.30.0.0/16. If you change this value, all the subnet parameters above may need to be set, as well.|

### <a name="PreloadFSx"></a>  Preload A2D2 data from S3 to FSx  

*This step can be executed anytime after "Configure the data service infrastructure" step has been executed*

Amazon FSx for Lustre automatically lazy loads data from the configured S3 bucket. Therefore, this step is strictly a performance optimization step . However, for maximal performance, it is *highly recommended!*
 

Execute following command to start preloading data from your S3 bucket to the FSx file-system:

	kubectl apply -n a2d2 -f a2d2/fsx/stage-data-a2d2.yaml

Execute following command to verify data is being copied to FSx for Lustre correctly:

	kubectl logs -f stage-fsx-a2d2 -n a2d2
	
This step will take several hours to complete. To check if the step is complete, execute:

	kubectl get pods stage-fsx-a2d2 -n a2d2

If the pod is still ```Running```, the step has not yet completed. This step takes approximately 9 hours to complete.


### <a name="PreloadEFS"></a> Preload A2D2 data from S3 to EFS 

*This step can be executed anytime after "Configure the data service infrastructure" step has been executed*

On first walk through of the tutorial, skip this step. This step is required **only** if you plan to configure the data service to use EFS as the raw data input source, otherwise, it may be safely skipped. 

Execute following command to start preloading data from your S3 bucket to the EFS file-system:

	kubectl apply -n a2d2 -f a2d2/efs/stage-data-a2d2.yaml

Execute following command to verify data is being copied to EFS correctly:

	kubectl logs -f stage-efs-a2d2 -n a2d2
	
This step will take several hours to complete. To check if the step is complete, execute:

	kubectl get pods stage-efs-a2d2 -n a2d2

If the pod is still ```Running```, the step has not yet completed. This step takes approximately 9 hours to complete.





