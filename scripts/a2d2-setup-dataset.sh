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
 
[[ ! -z $(helm list | grep a2d2-data-service) ]] && echo "Stop running a2d2-data-service" && exit 1
[[ ! -z $(helm list | grep a2d2-rosbridge) ]] && echo "Stop running a2d2-rosbridge" && exit 1

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$scripts_dir/..
[[ -f "$DIR/.ecr_uri" ]] || (echo "Expected $DIR/.ecr_uri file not found" && exit 1)

ecr_uri=$(cat "$DIR/.ecr_uri")
[[ -z "${ecr_uri}" ]] && echo "ecr_uri variable required" && exit 1

cd $scripts_dir && python3 get-ssm-params.py && source setenv.sh

# set aws region
aws_region=$(aws configure get region)
[[ -z "${aws_region}" ]] && echo "aws_region env variable is required" && exit 1
[[ -z "${s3_bucket_name}" ]] && echo "s3_bucket_name env variable is required" && exit 1
[[ -z "${redshift_cluster_host}" ]] && echo "redshift_cluster_host variable required" && exit 1
[[ -z "${redshift_cluster_username}" ]] && echo "redshift_cluster_username variable required" && exit 1
[[ -z "${redshift_cluster_dbname}" ]] && echo "redshift_cluster_dbname variable required" && exit 1
[[ -z "${redshift_cluster_password}" ]] && echo "redshift_cluster_password variable required" && exit 1
[[ -z "${eks_pod_sa_role_arn}" ]] && echo "eks_pod_sa_role_arn variable required" && exit 1
[[ -z "${eks_node_role_arn}" ]] && echo "eks_node_role_arn variable required" && exit 1
[[ -z "${data_request_table}" ]] && echo "data_request_table variable required" && exit 1


DATE=`date +%s`
# update helm charts values.yaml for a2d2-rosbridge
sed -i -e "s/\"host\": .*/\"host\": \"${redshift_cluster_host}\",/g" \
    -e "s/\"user\": .*/\"user\": \"${redshift_cluster_username}\",/g" \
    -e "s/\"password\": .*/\"password\": \"${redshift_cluster_password}\",/g" \
    -e "s/\"rosbag_bucket\": .*/\"rosbag_bucket\": \"${s3_bucket_name}\",/g" \
    -e "s/\"cal_bucket\": .*/\"cal_bucket\": \"${s3_bucket_name}\",/g" \
    -e "s|roleArn:.*|roleArn: ${eks_pod_sa_role_arn}|g" \
    -e "s|\"data_request_table\":.*|\"data_request_table\": \"${data_request_table}\"|g" \
    -e "s|uri:.*|uri: ${ecr_uri}|g" \
    $DIR/a2d2/charts/a2d2-rosbridge/values.yaml

if [[ ! -z "${msk_cluster_arn}" ]]
then
MSK_SERVERS=$(aws kafka --region ${aws_region} get-bootstrap-brokers \
        	--cluster-arn ${msk_cluster_arn} | \
            grep \"BootstrapBrokerString\"  | \
            awk '{split($0, a, " "); print a[2]}')

# update helm charts values.yaml for a2d2-data-service and example client config files
sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
    -e "s/\"host\": .*/\"host\": \"${redshift_cluster_host}\",/g" \
    -e "s/\"user\": .*/\"user\": \"${redshift_cluster_username}\",/g" \
    -e "s/\"password\": .*/\"password\": \"${redshift_cluster_password}\",/g" \
    -e "s/\"rosbag_bucket\": .*/\"rosbag_bucket\": \"${s3_bucket_name}\",/g" \
    -e "s/\"cal_bucket\": .*/\"cal_bucket\": \"${s3_bucket_name}\",/g" \
    -e "s|roleArn:.*|roleArn: ${eks_pod_sa_role_arn}|g" \
    -e "s|\"data_request_table\":.*|\"data_request_table\": \"${data_request_table}\"|g" \
    -e "s|uri:.*|uri: ${ecr_uri}|g" \
    $DIR/a2d2/charts/a2d2-data-service/values.yaml

sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
        $DIR/a2d2/config/c-config-ex1.json
                  
sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
        $DIR/a2d2/config/c-config-ex2.json

sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
        $DIR/a2d2/config/c-config-ex3.json

sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
        $DIR/a2d2/config/c-config-ex4.json

sed -i -e "s/\"servers\": .*/\"servers\": $MSK_SERVERS/g" \
        $DIR/a2d2/config/c-config-lidar.json

fi

# Update yaml files for creating EFS persistent-volume
sed -i -e "s/volumeHandle: .*/volumeHandle: ${efs_id}/g" \
    $DIR/a2d2/efs/pv-efs-a2d2.yaml

# create a2d2 namespace if needed
kubectl get namespace a2d2 || kubectl create namespace a2d2

# deploy AWS EFS CSI driver
echo "Deploy AWS EFS CSI Driver"
$scripts_dir/deploy-efs-csi-driver.sh
kubectl apply -f $DIR/a2d2/efs/efs-sc.yaml

# create EFS persistent volume
echo "Create k8s persistent-volume and persistent-volume-claim for efs"
kubectl apply -n a2d2 -f $DIR/a2d2/efs/pv-efs-a2d2.yaml
kubectl apply -n a2d2 -f $DIR/a2d2/efs/pvc-efs-a2d2.yaml

if [[ ! -z ${fsx_id} ]] && [[ ! -z ${fsx_mount_name} ]]
then

# deploy AWS FSx CSI driver
echo "Deploy AWS FSx CSI Driver"
$scripts_dir/deploy-fsx-csi-driver.sh

sed -i -e "s/volumeHandle: .*/volumeHandle: ${fsx_id}/g" \
    -e "s/dnsname: .*/dnsname: ${fsx_id}.fsx.${aws_region}.amazonaws.com/g" \
    -e "s/mountname: .*/mountname: ${fsx_mount_name}/g"  \
	$DIR/a2d2/fsx/pv-fsx-a2d2.yaml

# create FSx persistent volume
echo "Create k8s persistent-volume and persistent-volume-claim for fsx"
kubectl apply -n a2d2 -f $DIR/a2d2/fsx/pv-fsx-a2d2.yaml
kubectl apply -n a2d2 -f $DIR/a2d2/fsx/pvc-fsx-a2d2.yaml

# uncomment fsx related configuration
sed -i -e "s/\"input\": *\"s3\"/\"input\": \"fsx\"/" \
    $DIR/a2d2/charts/a2d2-rosbridge/values.yaml

sed -i -e "s/\"input\": *\"s3\"/\"input\": \"fsx\"/" \
    $DIR/a2d2/charts/a2d2-data-service/values.yaml

sed -i -e '/^#\+[^#]*- *name: \+fsx/ {s/^#\+//;n;s/^#\+//;n;s/^#\+//}' \
    -e '/^#\+[^#]*- *mountPath: \+\/fsx/ {s/^#\+//;n;s/^#\+//}' \
    $DIR/a2d2/charts/a2d2-rosbridge/templates/a2d2.yaml

sed -i -e '/^#\+[^#]*- *name: \+fsx/ {s/^#\+//;n;s/^#\+//;n;s/^#\+//}' \
    -e '/^#\+[^#]*- *mountPath: \+\/fsx/ {s/^#\+//;n;s/^#\+//}' \
    $DIR/a2d2/charts/a2d2-data-service/templates/a2d2.yaml


else

#delete FSx persistent volume
echo "Delete k8s persistent-volume and persistent-volume-claim for fsx"
kubectl delete -n a2d2 -f $DIR/a2d2/fsx/pv-fsx-a2d2.yaml
kubectl delete -n a2d2 -f $DIR/a2d2/fsx/pvc-fsx-a2d2.yaml

# comment out fsx related configuration
sed -i -e "s/\"input\": *\"fsx\"/\"input\": \"s3\"/" \
    $DIR/a2d2/charts/a2d2-rosbridge/values.yaml

sed -i -e "s/\"input\": *\"fsx\"/\"input\": \"s3\"/" \
    $DIR/a2d2/charts/a2d2-data-service/values.yaml

sed -i -e '/^[^#]*- *name: \+fsx/ {s/^/#/;n;s/^/#/;n;s/^/#/}' \
    -e '/^[^#]*- *mountPath: \+\/fsx/ {s/^/#/;n;s/^/#/}' \
    $DIR/a2d2/charts/a2d2-rosbridge/templates/a2d2.yaml

sed -i -e '/^[^#]*- *name: \+fsx/ {s/^/#/;n;s/^/#/;n;s/^/#/}' \
    -e '/^[^#]*- *mountPath: \+\/fsx/ {s/^/#/;n;s/^/#/}' \
    $DIR/a2d2/charts/a2d2-data-service/templates/a2d2.yaml


fi

# Update eks pod sa role in yaml files used for staging data
sed -i -e "s|eks\.amazonaws\.com/role-arn:.*|eks.amazonaws.com/role-arn: ${eks_pod_sa_role_arn}|g" \
    -e "s|value:[[:blank:]]\+$|value: ${s3_bucket_name}|g" $DIR/a2d2/efs/stage-data-a2d2.yaml

sed -i -e "s|eks\.amazonaws\.com/role-arn:.*|eks.amazonaws.com/role-arn: ${eks_pod_sa_role_arn}|g" \
    -e "s|value:[[:blank:]]\+$|value: ${s3_bucket_name}|g" $DIR/a2d2/fsx/stage-data-a2d2.yaml

## show all the persistent volume claims
kubectl get pvc -n a2d2