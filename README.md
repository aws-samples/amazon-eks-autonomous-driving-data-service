# Data Service for ADAS and ADS Development

## Overview

This is an example of a data service typically used in advanced driver assistance systems (ADAS) and automated driving systems (ADS) development. The typical use case addressed by this data service is to aggregate and serve a series of [rosbag](http://wiki.ros.org/rosbag) files containing data that spans a drive-scene of interest. Each ```rosbag``` file in the series contains a discreet segment of the overall requested drive-scene data. Each ```rosbag``` file is aggregated from decomposed drive-scene  sensor data stored in [Amazon S3](https://aws.amazon.com/free/storage/s3), and meta-data stored in [Amazon Redshift](https://aws.amazon.com/redshift/). The drive-scene data of interest is identified by querying the meta-data stored in Amazon Redshift. 

While this specific data service uses [rosbag](http://wiki.ros.org/rosbag) file-format for returning the aggregated drive-scene data, the general concepts and ideas that inform this data service are not anchored in any specific file-format, and can be easily adapted to other file-formats.

### Key concepts

The data service runs in [Kubernetes Pods](https://kubernetes.io/docs/concepts/workloads/pods/) in an [Amazon EKS](https://aws.amazon.com/eks/) cluster. An [Amazon Managed Service For Apache Kafka](https://aws.amazon.com/msk/) (MSK) cluster provides the communication channel between data client and the data service. The data service implements a reqest-response paradigm over Kafka topics. For scalability, multiple data service pods may listen for requests on the same Kafka topic, and multiple Kafka topics may be used by the data service to serve different data sets. When a data request arrives on a Kafka topic, one of the data service pods listening on the topic receives and processes the request.

It is assumed that the data client knows the Kafka cluster endpoint, and the name of a Kafka topic on which the data service is listening. The data client sends requests for data on the well-known Kafka topic. The data-client request is encapsulated in a JSON document, and includes a unique dynamically generated response Kafka topic name. After sending the JSON request, the data client waits for the response on the unique response Kafka topic contained in the request.

The data service processes the data requests it receives and sends back the requested data to the data client. The data sent back to the client is staged on a shared file-system on FSx, or EFS, or in an S3 bucket, as requested by the data client. The staged data location is communicated to the data client in a JSON message sent on the unique response Kafka topic. If the data client requests to use FSx, or EFS shared file systems for staging the data, it is assumed that the data service and the data client are mounting the shared FSx, or EFS file systems at the same file-system path: ```/fsx```, or ```/efs```, respectively, otherwise the returned data location would be meaningless to the data client. 

Once the data client receives the message containing the data location over the unique Kafka response topic, the data client directly reads the data, and processes the data as it deems fit. Once the returned data is processed, the data client deletes the data, and the unique Kafka response topic.

Concretely, imagine you want to request ```rosbag``` files aggregating drive scene  data from [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html) spanning a specific time period, and you want the ```rosbag``` files to include data  from the front-left camera and lidar sensors, only. You can articulate such a request using a data client JSON document shown below:

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

Below, we explain the semantics of the various fields in the data client example JSON document shown above:

* The ```servers``` identify the Kafka cluster endpoint
* The JSON document includes an array of one or more data ```requests```
* The ```kafka_topic``` specifies the pre-shared Kafka topic on which the data request is sent
* The ```vehicle_id``` is used to identify the relevant drive-scene data set 
* The ```scene_id``` identifies the drive scene of interest, which in this example is ```20190401145936```, presumably a string representing the date and time of the drive
* The ```start_ts``` and ```stop_ts``` (in microseconds) specify the start and stop timestamps for the drive scene data of interest
* The ```ros_topic``` is a map from sensors in the vehicle to ```ros``` topics, and the ```data_type``` is a map from sensors to ```ros``` data types. 
* The ```step``` is the discreet time step (in microseconds) used to divide up the total requested timespan into discreet chunks for the purpose of reading meta-data from RedShift database, and aggregating data in a ```rosbag``` file
* The ```accept``` specifies format expected by the client:
	* For example, ```fsx/multipart/rosbag``` means data should be aggregated into multiple discreet rosbag files, one ```rosbag``` file for each ```step```, and data should be staged on a shared [Amazon FSx for Lustre](https://aws.amazon.com/fsx/lustre/) file-system. 
	* Optionally, one can specify ```accept``` as ```efs/multipart/rosbag``` for using [Amazon EFS](https://aws.amazon.com/efs/), or ```s3/multipart/rosbag``` for using [Amazon S3](https://aws.amazon.com/s3/) to stage the ```rosbag``` files
	* If ```accept``` specifies ```singlepart```, the requested data is aggregated into a single ```rosbag``` file. For data requests spanning a large interval, such a request can take several minutes to be completed
	* if ```accept``` is specified as ```manifest```, ordered manifest data containing S3 paths for the raw data (not aggregated ```rosbag``` files) matching the data request is streamed over the unique response Kafka topic, and the data client is expected to process the manifest stream as it deems fit 
* If ```preview``` field is set to ```true```, the data service returns requested data over a single ```step``` starting with```start_ts```

### Data input source, and response data staging

The data service can be configured to input data from S3, FSx, or EFS.  Similarly, the data client can request the response data to be staged on S3, FSx, or EFS. Any combination of data input source and response data staging is valid. 

For maximum performance, pre-load the data in FSx file-system, and use FSx for data input source, and response data staging. Next best option is to use FSx for data input source (without pre-loading of data), and use FSx for response data staging. If you use FSx as data input source without pre-loading of data, first time requests for data are slightly slower than subsequent requests, because FSx automatically lazy loads data from S3, and caches the data on the FSx file-system. 

If you want to use EFS file-system as data input source, you must pre-load the data to the file-system. EFS does not automatically load data from S3. For an optimal combination of cost, setup time and performance, try different options, and decide what best meets the objectives of your use case.

## Tutorial steps

In this tutorial, we focus on [A2D2 autonomous driving dataset](https://www.a2d2.audi/a2d2/en.html). However, the data service can be customized to work with other datasets.

The general outline of this tutorial is as follows:
* Setup EC2 developer instance
* Create an S3 bucket and copy [A2D2 dataset](https://registry.opendata.aws/aev-a2d2/) to your Amazon S3 bucket
* Use [AWS CloudFormation](https://aws.amazon.com/cloudformation/) to create the infrastructure 
	* Install and configure ```kubectl```
	* Install ```eksctl```
	* Install ```Helm```
	* Deploy AWS EFS CSI Driver
	* Deploy AWS FSx CSI Driver
	* Create EFS Persistent Volume
	* Create FSx Persistent Volume
	* Update [Amazon MSK](https://aws.amazon.com/msk/) cluster configuration
* Create ```a2d2``` schema and tables in [Amazon Redshift](https://aws.amazon.com/Redshift/) 
* Load initial data to ```a2d2``` schema tables
* Create [AWS Glue Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint.html) and an attached Jupyter Notebook instance
	* Extract A2D2 meta-data to CSV files in S3 bucket
	* Load extracted meta-data to Amazon Redshift
* Create [AWS IAM](https://aws.amazon.com/iam/) Role for EKS pods
* Optionally, stage A2D2 data on EFS
* Optionally, stage A2D2 data on FSx for Lustre
* Build and push Docker container image to [Amazon ECR](https://aws.amazon.com/ecr/)
* Use Helm chart to start data service
* Launch graphics desktop for vizualization of ```rosbag``` files using [rviz](http://wiki.ros.org/rviz)


## Setup EC2 developer instance

To get started, you will need an EC2 key pair. If you have not already created an EC2 key pair, [create a new EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). 

Launch [Ubuntu 18.04 AMI](https://aws.amazon.com/marketplace/pp/B07CQ33QKV?qid=1596551002060&sr=0-1&ref_=srh_res_product_title) instance [using EC2 console](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html). Select ```m5a.4xlarge``` instance type. Select ```gp2``` [EBS volume](https://aws.amazon.com/ebs/) with atleast 500 GB.  Once the instance is Running, [connect to your EC2 developer instance using SSH](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html). 

Clone this git repository on the EC2 instance. **For all the commands in this tutorial, we assume the current working directory to be the root of the cloned git repository.** Next, we setup the EC2 developer instance:

	./scripts/setup-dev.sh

Before we proceed, you must **logout** of the EC2 instance and connect again using SSH. 

This tutorial requires [AWS credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) for programmatic access consistent with [Network Administrator](https://docs.aws.amazon.com/en_pv/IAM/latest/UserGuide/access_policies_job-functions.html) job function. After you create AWS Access keys for programmatic access with requisite permissions consistent with Network job function, [configure AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) on EC2 developer instance:
	
	aws configure


## Create S3 bucket and copy A2D2 dataset
Using AWS CLI, we next create an S3 bucket. If your region is ```us-east-1```, execute:

	aws s3api create-bucket --bucket <bucket-name> --region us-east-1

For all other regions, execute:

	aws s3api create-bucket --bucket <bucket-name> --region <aws-region> --create-bucket-configuration LocationConstraint=<aws-region>

To copy [A2D2 dataset](https://registry.opendata.aws/aev-a2d2/) from ```aev-autonomous-driving-dataset``` S3 bucket to your S3 bucket under ```a2d2``` prefix, specify your ```s3-bucket-name``` name below:

	nohup ./scripts/copy-a2d2.sh <s3-bucket-name> 1>/tmp/copy-a2d2.log 2>&1 &

Wait for this step to be completed before proceeding. This step may take upto 24 hours to complete.

## Use AWS CloudFormation to create the infrastructure
Using [CloudFormation console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html) create a CloudFormation stack using [mozart.yml](cfn/mozart.yml) template. Following CloudFormation parameters are required:
  * ```S3Bucket```: This is the name of your S3 bucket
  * ```KeyPairName```: This is the Amazon EC2 SSH Key pair name you created above
  * ```RedshiftMasterUserPassword```: Specify a password for Redshift ```admin``` user

For maximum security, it is highly recommended that you restrict SSH access to the graphics desktop you may launch later in the tutorial by setting ```RemoteAccessCIDR``` parameter to your specific Internet IP ```/32``` CIDR address.
 
 Wait for CloudFormation Stack status to show ```Completed``` before proceeding. You will need the ```Output``` of the CloudFormation stack in steps below. This step may take 30 minutes, or longer.

 If you have already created a stack using this template in this AWS region, you may need to update other parameters, so they may have unique values. This includes ```EKSClusterName``` parameter that specifies Amazon EKS cluster name created in the stack.
 
### Install and configure ```kubectl```
The CloudFormation stack created in the previous step includes an Amazon EKS cluster. The  default EKS cluster name is ```mozart```, unless you specified a different parameter value for ```EKSClusterName``` in the previous step. We need to install and configure EKS cluster client, ```kubectl```, on EC2 developer instance. This will allow us to communicate with EKS cluster. To install ```kubectl```, first ```ssh``` to EC2 developer instance, and execute:

	./scripts/install-kubectl.sh <aws-region> <eks-cluster-name>

Verify ```kubectl``` installed correctly:

	kubectl get svc

Create ```a2d2``` namespace:

	kubectl create namespace a2d2

Secure kube config file:

	chmod go-rwx ~/.kube/config
	
### Install ```eksctl```

To install ```eksctl```, execute:

	./scripts/install-eksctl.sh

Next,  to configure open id provider in our EKS cluster, execute:

	eksctl utils associate-iam-oidc-provider --region <aws-region> --name <eks-cluster-name> --approve

### Install Helm
We plan to use [Helm](https://docs.aws.amazon.com/eks/latest/userguide/helm.html) with EKS. To install Helm:

	./scripts/install-helm.sh

### Deploy AWS EFS CSI Driver
To deploy [AWS EFS CSI Driver](https://github.com/kubernetes-sigs/aws-efs-csi-driver) execute:

	./scripts/deploy-efs-csi-driver.sh
	kubectl apply -f a2d2/efs/efs-sc.yaml
	
### Deploy AWS FSx CSI Driver
To deploy [AWS FSX CSI Driver](https://github.com/kubernetes-sigs/aws-fsx-csi-driver) execute:

	./scripts/deploy-fsx-csi-driver.sh
	
### Create EFS Persistent Volume

For this step, we will need the ```EFSFileSystemId``` from the output of the CloudFormation stack we created above. Specify ```<stack-name>``` below, and note the ```EFSFileSystemId``` in the output of the command:

	aws cloudformation describe-stacks --stack-name <stack-name>

Edit ```a2d2/efs/pv-efs-a2d2.yaml``` specifying ```EFSFileSystemId``` as the value of the ```volumeHandle```. Execute:

	kubectl apply -n a2d2 -f a2d2/efs/pv-efs-a2d2.yaml

Verify persistent-volume was successfully created: 
	
	kubectl get pv -n a2d2

Create EFS persistent-volume-claim: 
	
	kubectl apply -n a2d2 -f a2d2/efs/pvc-efs-a2d2.yaml

Verify persistent-volume was successfully bound: 
	
	kubectl get pv -n a2d2

### Create FSx Persistent Volume

For this step we will need the ```FSxFileSystemId``` in CloudFormation stack we created above. Specify ```<stack-name>``` below, and note the ```FSxFileSystemId``` in the output of the command:

	aws cloudformation describe-stacks --stack-name <stack-name>

We also need the FSx file-system mount name. To get that, specify ```<FSxFileSystemId>``` below and note ```MountName``` and ```DNSName``` in the output of the command:

	aws fsx describe-file-systems --file-system-ids <FSxFileSystemId>
	
Edit ```a2d2/fsx/pv-fsx-a2d2.yaml```, set ```volumeHandle``` to ```FSxFileSystemId```, set ```mountname```  to ```MountName```, and set ```dnsname``` to ```DNSName```. Execute:

	kubectl apply -n a2d2 -f a2d2/fsx/pv-fsx-a2d2.yaml

Verify persistent-volume was successfully created: 
	
	kubectl get pv -n a2d2

Create FSx persistent-volume-claim: 
	
	kubectl apply -n a2d2 -f a2d2/fsx/pvc-fsx-a2d2.yaml

Verify persistent-volume was successfully bound: 
	
	kubectl get pv -n a2d2

### Update Amazon MSK cluster configuration

The CloudFormation stack you created above created your [Amazon MSK](https://aws.amazon.com/msk/) cluster. This step creates a new Amazon MSK cluster configuration and updates the configuration of your Amazon MSK cluster. This step can be done using AWS CLI, as described below, or using AWS management console (recommended). 

Create a [new Amazon MSK cluster configuration](https://docs.aws.amazon.com/msk/latest/developerguide/msk-configuration-operations.html#msk-configuration-operations-create) with following settings:

```
auto.create.topics.enable=true
delete.topic.enable=true
num.replica.fetchers=2
socket.request.max.bytes=104857600
unclean.leader.election.enable=true
default.replication.factor=2
min.insync.replicas=2
num.io.threads=16
num.network.threads=10
num.partitions=1
log.roll.ms=900000 
log.retention.ms=1800000
```

[Update the configuration of your Amazon MSK cluster](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-cluster-config.html) with the cluster configuration created above. 

## Create a2d2 Redshift schema and tables 
For this step, use the AWS management console built-in [query editor](https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor.html). To use the query editor, you will need to connect to the Amazon Redshift database (default name of database is ```mozart```) using the ```username``` (default ```username``` is ```admin```) and ```password``` you used to create the RedShift database.  Once connected to the Redshift database, execute following SQL statement in the query editor to create ```a2d2``` schema:

	create schema a2d2

Next, execute the SQL in [sensor.ddl](a2d2/ddl/sensor.ddl), [vehicle.ddl](a2d2/ddl/vehicle.ddl), and [drive_data.ddl](a2d2/ddl/drive_data.ddl), **in order**, in the AWS management console built-in [query editor](https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor.html) to create new Redshift tables for  ```sensor```, ```vehicle```, and ```drive_data```, respectively. 

## Load initial data to a2d2 schema tables

Next, upload [sensors.csv](data/sensors.csv) and [vehicle.csv](data/vehicle.csv) files to your S3 bucket, by executing following AWS CLI commands in the root directory of your Git repository on your EC2 developer instance:

	aws s3 cp a2d2/data/sensors.csv s3://<your-s3-bucket>/redshift/sensors.csv
	aws s3 cp a2d2/data/vehicle.csv s3://<your-s3-bucket>/redshift/vehicle.csv

For the next step, you need to note down ```RedshitClusterRole``` available in the output of the command below:

	aws cloudformation describe-stacks --stack-name <stack-name>

In the Redshift query editor, use the ```RedshiftClusterRole``` your noted above as the ```iam_role```, and run the statement below:

	COPY a2d2.sensor
	FROM 's3://<your-s3-bucket>/redshift/sensors.csv'
		iam_role  'arn:aws:iam::XXXXXXXXXXXX:role/xxxxxxx-RedshiftClusterRole-XXXXXXXXXXXX'
 		CSV
		
Next, in the Redshift query editor, run the statement below:

	COPY a2d2.vehicle
	FROM 's3://<your-s3-bucket>/redshift/vehicle.csv'
		iam_role  'arn:aws:iam::XXXXXXXXXXXX:role/xxxxxx-RedshiftClusterRole-XXXXXXXXXXXX'
 		CSV

## Create AWS Glue development endpoint and notebook instance

In this step you will [Add an AWS Glue Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/add-dev-endpoint.html) and then [create an Amazon SageMaker Notebook with your AWS Glue Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-sage.html). You will create an IAM role as part of adding an AWS Glue Endpoint, and another IAM role as part of creating a SageMaker notebook. Both these IAM roles need full access to your S3 bucket. So, edit these roles in AWS management console, and add following IAM inline policy to these roles (replace ```your-s3-bucket-name``` with your S3 bucket name):

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:List*",
                "s3:Get*",
                "s3:PutObject*",
                "s3:DeleteObject*"
            ],
            "Resource": [
                "arn:aws:s3:::your-s3-bucket-name",
                "arn:aws:s3:::your-s3-bucket-name/*"
            ]
        }
    ]
}
```

Execute following steps to create the AWS Glue development endpoint and associated notebook instance:

  -  [Add an AWS Glue Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/add-dev-endpoint.html)
      - Under "Security configuration, script libraries, and job parameters (optional)" in the wizard
      	- use Worker type as G.2X  (minimum 5 workers) and use latest Glue version with support for Python 3
      - Skip "Networking" in the wizard
      - Skip "Add an SSH public key (Optional)" in the wizard
      - Create a Glue service IAM role and edit role to add the S3 bucket inline policy shown above
    
  -  [Create Amazon SageMaker Notebook with your AWS Glue Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-sage.html)
     - Create an IAM role as part of the create notebook wizard and edit the role to add S3 bucket inline policy
     
Open the Amazon SageMaker notebook instance you just created. If you prefer using JuypyterLab. switch the notebook instance to JupyterLab. Open a terminal in notebook instance. Clone this Git repository under ```$HOME/SageMaker``` directory in the notebook instance.
  
### Extract A2D2 meta-data to CSV files in S3 bucket
In the Amazon SageMaker notebook instance you created above, open ```mozart/a2d2/notebooks/a2d2-camera-lidar-json.ipynb``` notebook and run through it. This notebook uses PySpark to read relevant A2D2 data from your S3 bucket, transform it, and save the transformed data into CSV files in your S3 bucket. In the next step, we import the transformed CSV data files into ```a2d2.drive_data``` Redshift table.

### Load extracted meta-data to Amazon Redshift

In this step, we import the extracted CSV files from S3 into ```a2d2.drive_data``` Redshift table. We do this in two steps. In the Redshift query editor, first run the statement below (specifying your S3 bucket name and ```iam_role``` ):

	COPY a2d2.drive_data
	FROM 's3://<your-s3-bucket>/emr/a2d2/image/v1/'
		iam_role  'arn:aws:iam::XXXXXXXXXXXX:role/xxxxxx-RedshiftClusterRole-XXXXXXXXXXXX'
 		CSV
		IGNOREHEADER 1

Next, run the statement below:

	COPY a2d2.drive_data
	FROM 's3://<your-s3-bucket>/emr/a2d2/pcld/v1/'
		iam_role  'arn:aws:iam::XXXXXXXXXXXX:role/xxxxxx-RedshiftClusterRole-XXXXXXXXXXXX'
 		CSV
		IGNOREHEADER 1

## Create AWS IAM Role for EKS pods

Before we can proceed, we need to create an AWS IAM role that will allow various EKS pods to access your S3 bucket. 

Execute:

		./scripts/create-eks-sa-role.sh <eks-cluster-name> <s3-bucket-name>
		
to create the AWS IAM role. **Note the ```ROLE``` output of this command: you will need it in steps below.**

## (Optional step) Stage A2D2 data on EFS 

*This step is needed only if your plan to configure the data service to use EFS as the data input source. This step is not needed if you plan to  use EFS only with your data client for reading response data.* 

Edit ```a2d2/efs/stage-data-a2d2.yaml``` and set the value of ```eks.amazonaws.com/role-arn``` to the ```ROLE``` output in the step "Create AWS IAM role for EKS pods", and set ```S3_BUCKET``` environment variable to your S3 bucket name. 

Execute following command to start copying data from your S3 bucket to EFS:

	kubectl apply -n a2d2 -f a2d2/efs/stage-data-a2d2.yaml

Execute following command to verify data is being copied to EFS correctly:

	kubectl logs -f stage-efs-a2d2 -n a2d2
	
This step will take several hours to complete. To check if the step is complete, execute:

	kubectl get pods stage-efs-a2d2 -n a2d2

If the pod is still ```Running```, the step is not yet completed. This step takes approximately 14 hours to complete.

## (Recommended optional step) Stage A2D2 data on FSx for Lustre 

*FSx for Lustre automatically loads data from your S3 bucket when data is accessed for the first-time. Therefore, strictly speaking, this step is needed only if you plan to use FSx for Lustre as a data input source, and you want to pre-load A2D2 data to FSx for Lustre to accelerate performance on first-time data access. This step is recommended.*

Edit ```a2d2/fsx/stage-data-a2d2.yaml``` and set the value of ```eks.amazonaws.com/role-arn``` to the ```ROLE``` output in the step "Create AWS IAM role for EKS pods", and set ```S3_BUCKET``` environment variable to your S3 bucket name. 

Execute following command to start copying data from your S3 bucket to FSx for Lustre:

	kubectl apply -n a2d2 -f a2d2/fsx/stage-data-a2d2.yaml

Execute following command to verify data is being copied to FSx for Lustre correctly:

	kubectl logs -f stage-fsx-a2d2 -n a2d2
	
This step will take several hours to complete. To check if the step is complete, execute:

	kubectl get pods stage-fsx-a2d2 -n a2d2

If the pod is still ```Running```, the step is not yet completed. This step takes approximately 9 hours to complete.

## Build and push Docker container image to Amazon ECR

Before you execute this step, verify that you have access to ```docker``` daemon by executing this command:

		docker ps -a

If you get an error, this means you omitted to logout and login as noted above after the intial setup on the EC2 developer machine, so you may want to do that now.

Next, we need to build and push required Docker image. We can buiild a Docker image for ```melodic``` or ```noetic``` [ros distribuion](http://wiki.ros.org/Distributions). Set ```<aws-region``` to your AWS Region in commands below.

For ```melodic``` distribution, execute:

		cd a2d2 && ./build_tools/build_and_push.sh <aws-region> melodic-bionic && cd ..

For ```noetic``` distribution, execute:

		cd a2d2 && ./build_tools/build_and_push.sh <aws-region> noetic-focal && cd ..

Note the Amazon ECR URI for the docker image you just built: you will need it to configure the values for Helm charts in the steps below.

## Use Helm chart to start data service

For this step, we need to edit ```a2d2/charts/a2d2-data-service/values.yaml``` and set relevant values. Set ```a2d2.image.uri``` to your docker image's Amazon ECR URI. Set ```a2d2.serviceAccount.roleArn``` to the ```ROLE``` output in the step "Create AWS IAM role for EKS pods". Set ```configMap.servers``` to your plain-text [Amazon MSK](https://aws.amazon.com/msk/) cluster endpoint, available in Amazon MSK management console under MSK cluster client information. Set ```configMap.database.host``` to Redshift database endpoint host name (don't include port and database name), ```configMap.database.port``` to Redshift port, and ```configMap.database.password``` to your Redshift database password. Set ```configMap.data_store.input``` to ```fsx```, ```efs```, or ```s3```. For maximum performance option, use ```fsx```. For lowest cost option, use ```s3```. 

To start the A2D2 data service, execute:

		helm install --debug a2d2-data-service ./a2d2/charts/a2d2-data-service/

To verify that the ```a2d2-data-service``` pod is running, execute the command

		kubectl get pods -n a2d2
		
If you want to experiment with various data store input options, you will need to restart ```a2d2-data-service```. You can delete the running service by executing:

		helm delete a2d2-data-service

To run the A2D2 data service client, we will use a graphics EC2 desktop to run your A2D2 data service client, and use [rviz](http://wiki.ros.org/rviz) tool on the graphics desktop to visualize the data returned by the A2D2 data service.
 
## Launch graphics desktop for vizualization of rosbag files using rviz

In this step, we will need information from the CloudFormation stack output. Specify your ```<stack-name>``` in following command, and save the command output because you will need it below:

	aws cloudformation describe-stacks --stack-name <stack-name>
	
The actions below need you to use AWS Management Console in a browser, so they are best executed on your laptop, not on the EC2 developer machine we have been using so far. 

Next, we ```git clone``` this repository on your laptop in your home directory. After that, we launch [Nvidia Quadro Virtual Workstation - Ubuntu 18.04](https://aws.amazon.com/marketplace/pp/B07YV3B14W?qid=1599175893536&sr=0-1&ref_=srh_res_product_title) through EC2 console **in the same AWS region you have been working so far** with following configuration (see CloudFormation stack output):

* Select ```g4dn.2xlarge``` EC2 instance type
* Use the VPC created in the CloudFormation stack
* Use one of the public subnets created in the CloudFormation stack ( see "VpcPublicSubnets" in the CloudFormation stack output above)
* Set **Auto-assign Public IP** to "Enable"
* Set **IAM role** to ```XXXXX-DesktopInstanceProfile-XXXXXXXXXXXXX``` (see "DesktopInstanceProfile" in the CloudFormation stack output above)
* Under Advanced Details, select **User data** "As file" and "Choose file" ```scripts/desktop-melodic-bionic.sh``` from this repository as user data for the new EC2 instance
* Specify at least 100 GB EBS gp2 storage volume for root device
* Choose the existing security group ```XXXXXX-DesktopSecurityGroup-XXXXXXXXXXXX``` (see "DesktopSecurityGroup" in the CloudFormation stack output above)
* Select the same key pair you used when you created your EC2 developer instance above
	
After the desktop instance is launched and ```Running```, wait at least 10 minutes before logging in. This will allow sufficient time for user data script execution to complete.

Next, we need to execute following steps:

* ```ssh``` into the desktop. 
* Execute ```sudo passwd ubuntu``` to set a new password for user ```ubuntu```. 
* Clone this ```git``` repository under user ```ubuntu``` home directory, and ```cd``` to the directory containing this repository.
* Install the latest FSx for Lustre client modules by executing: ```sudo apt install -y lustre-client-modules-$(uname -r)```
* Execute ```sudo mkdir /fsx```
* Using instructions available in AWS FSx management console, attach the FSx for Lustre file-system created in the CloudFormation stack on ```/fsx``` directory on the EC2 desktop.
* Optionally, if you are planning to use EFS file system:
	* ```sudo mkdir /efs```
	* Using instructions available in AWS EFS management console, attach the EFS file-system created in the CloudFormation stack on ```/efs``` directory on the EC2 desktop. 
* Logout from SSH session. 
* Install [NICE DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html) on your laptop and use the DCV client to login to the graphics desktop instance as user ```ubuntu```. Use the password for user ```ubuntu``` that you created above.

* On the graphics desktop, use the terminal to start ```rviz``` and set ```./a2d2/config/a2d2.rviz``` as the ```rviz``` config file. 
* Create a client configuration file starting as described below:

		cp /a2d2/config/c-config-ex1.json /tmp/c-config.json
  
  Edit the ```servers``` in ```/tmp/c-config.json``` file to the Amazon MSK plaintext endpoint. This should be the same endpoint you used to configure your data service above. Set ```requests.preview``` to ```false``` if you want to see complete data.
* Run A2D2 data service client using the command:

		python ./a2d2/src/data_client.py --config /tmp/c-config.json 1>/tmp/a.out 2>&1 & 
	

### Killing data service client

If you kill the data service client before it exits normally, be sure to clean up all the processes spawned by the data client. Killing the client does not stop the data service from sending the data to the client, as the data information is being sent asynchronously over a Kafka topic. If the client is aborted by the user, the data service will still send back the requested data and the data will remain stored on the output data store specified in the ```requests.accept``` in client configuration file.

