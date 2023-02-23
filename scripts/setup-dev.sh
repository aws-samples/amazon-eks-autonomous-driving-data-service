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
 
[[ ! -z $(helm list | grep data-service) ]] && echo "Stop running services" && exit 1
[[ ! -z $(helm list | grep rosbridge) ]] && echo "Stop running services " && exit 1

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$scripts_dir/..
cd $scripts_dir && python3 get-ssm-params.py && source setenv.sh

# check to see if kubectl is already configured
kubectl get svc || $scripts_dir/configure-eks-auth.sh

# set aws region
aws_region=$(aws configure get region)
[[ -z "${aws_region}" ]] && echo "aws_region env variable is required" && exit 1
[[ -z "${msk_cluster_arn}" ]] && echo "MSK cluster is not defined: WebSocket client only."

if [[ ! -z "${msk_cluster_arn}" ]]
then
MSK_SERVERS=$(aws kafka --region ${aws_region} get-bootstrap-brokers \
        	--cluster-arn ${msk_cluster_arn} | \
            grep \"BootstrapBrokerString\"  | \
            awk '{split($0, a, " "); print a[2]}')


# Create kafka.config 
cat >$DIR/adds/config/kafka.config <<EOL
{
    "config-name": "adds",
    "config-description": "ADDS Kafka configuration",
    "cluster-arn": "${msk_cluster_arn}",
    "cluster-properties": "$DIR/adds/config/kafka-cluster.properties"
}
EOL

chown ubuntu:ubuntu $DIR/adds/config/kafka.config

#Update MSK cluster config
echo "Update MSK cluster configuration"
python3 $scripts_dir/update-kafka-cluster-config.py --config $DIR/adds/config/kafka.config
fi

# deploy metrics server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# deploy eks cluster autoscaler
$scripts_dir/eks-cluster-autoscaler.sh

# Build ECR image
$scripts_dir/build-ecr-image.sh

if [[ $ROS_DISTRO == 'melodic' || $ROS_DISTRO == 'noetic' ]]
then
# Build catkin_ws
cd $DIR/adds/catkin_ws && catkin_make
echo "source /home/ubuntu/amazon-eks-autonomous-driving-data-service/adds/catkin_ws/devel/setup.bash" >> /home/ubuntu/.bashrc
else
# Build colcon_ws
cd $DIR/adds/colcon_ws && colcon build
echo "source /home/ubuntu/amazon-eks-autonomous-driving-data-service/adds/colcon_ws/install/setup.bash" >> /home/ubuntu/.bashrc
fi

## Execute setup dataset scripts
for setup_dataset_script in `ls $scripts_dir/*-setup-dataset.sh`
do
    [[ -x $setup_dataset_script ]] && $setup_dataset_script
done

## show all the persistent volumes
kubectl get pv