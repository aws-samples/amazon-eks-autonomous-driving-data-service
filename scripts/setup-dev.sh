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

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ ! -f ~/.aws/credentials ]] 
then
	aws configure
fi

# set aws region
aws_region=$(aws configure get region)
[[ -z "${aws_region}" ]] && echo "aws_region env variable is required" && exit 1
echo "AWS Region: $aws_region"

# set s3 bucket name
[[ -z "${s3_bucket_name}" ]] && echo "s3_bucket_name env variable is required" && exit 1
echo "S3 Bucket Name: $s3_bucket_name"

# set eks cluster name
[[ -z "${eks_cluster_name}" ]] && echo "eks_cluster_name env variable is required" && exit 1
echo "EKS Cluster Name: $eks_cluster_name"

# configure kubectl
echo "configure kubectl"
aws sts get-caller-identity
aws eks --region $aws_region update-kubeconfig --name $eks_cluster_name

# verify kubectl works
kubectl get svc || { echo 'kubectl configuration failed' ; exit 1; }
chmod go-rwx $HOME/.kube/config

# update aws-auth config map
if [[ -f $scripts_dir/../a2d2/config/aws-auth.yaml ]]
then
	echo "Apply configmap: $scripts_dir/../a2d2/config/aws-auth.yaml"
	kubectl apply -f $scripts_dir/../a2d2/config/aws-auth.yaml -n kube-system
fi

# create a2d2 namespace
kubectl create namespace a2d2

# configure open id provider in our EKS cluster
echo "Create EKS cluster IAM OIDC provider"
eksctl utils associate-iam-oidc-provider --region $aws_region --name $eks_cluster_name --approve

# deploy AWS EFS CSI driver
echo "Deploy AWS EFS CSI Driver"
$scripts_dir/deploy-efs-csi-driver.sh
kubectl apply -f $scripts_dir/../a2d2/efs/efs-sc.yaml

# deploy AWS FSx CSI driver
echo "Deploy AWS FSx CSI Driver"
$scripts_dir/deploy-fsx-csi-driver.sh

# create EFS persistent volume
echo "Create k8s persistent-volume and persistent-volume-claim for efs"
kubectl apply -n a2d2 -f $scripts_dir/../a2d2/efs/pv-efs-a2d2.yaml
kubectl apply -n a2d2 -f $scripts_dir/../a2d2/efs/pvc-efs-a2d2.yaml

# create FSx persistent volume
echo "Create k8s persistent-volume and persistent-volume-claim for fsx"
kubectl apply -n a2d2 -f $scripts_dir/../a2d2/fsx/pv-fsx-a2d2.yaml
kubectl apply -n a2d2 -f $scripts_dir/../a2d2/fsx/pvc-fsx-a2d2.yaml
kubectl get pv -n a2d2

# create k8s pod role
echo "Create EKS Pod role"
$scripts_dir/create-eks-sa-role.sh $eks_cluster_name $s3_bucket_name

#Update MSK cluster config
echo "Update MSK cluster configuration"
python3 $scripts_dir/update-kafka-cluster-config.py --config $scripts_dir/../a2d2/config/kafka.config

# remove credentials
rm  ~/.aws/credentials && echo "AWS Credentials Removed"