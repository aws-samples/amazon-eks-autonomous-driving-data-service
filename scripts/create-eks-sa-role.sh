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

CLUSTER_NAME=
BUCKET_NAME=
if [ "$#" -eq 2 ]; then
    CLUSTER_NAME=$1
    BUCKET_NAME=$2
else
    echo "usage: $0 <cluster-name> <bucket-name>"
    exit 1
fi


ISSUER_URL=$(aws eks describe-cluster \
                       --name $CLUSTER_NAME \
                       --query cluster.identity.oidc.issuer \
                       --output text)

# STEP 1: create IAM role and attach the target policy:
ISSUER_HOSTPATH=$(echo $ISSUER_URL | cut -f 3- -d'/')
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
PROVIDER_ARN="arn:aws:iam::$ACCOUNT_ID:oidc-provider/$ISSUER_HOSTPATH"

ROLE_NAME="eks-sa-${CLUSTER_NAME}-${BUCKET_NAME}-role"
cat > /tmp/$ROLE_NAME-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "$PROVIDER_ARN"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${ISSUER_HOSTPATH}:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
EOF

POLICY_NAME=${ROLE_NAME}-policy
cat > /tmp/${POLICY_NAME}.json << EOF
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
                "arn:aws:s3:::$BUCKET_NAME",
                "arn:aws:s3:::$BUCKET_NAME/*"
            ]
        }
    ]
}
EOF


aws iam create-policy \
	--policy-name ${POLICY_NAME} \
	--policy-document file:///tmp/${POLICY_NAME}.json \
	--output text


aws iam create-role \
          --role-name $ROLE_NAME \
          --assume-role-policy-document file:///tmp/$ROLE_NAME-trust-policy.json \
	  --output text

aws iam attach-role-policy --role-name ${ROLE_NAME} \
	  --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}" \
	  --output text
