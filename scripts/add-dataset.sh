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


[[ $# -ne 1 ]] && echo "usage: $0 dataset" && exit 1
dataset=$1
echo "Adding $dataset dataset"

scripts_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$scripts_dir/..

[[ -d $dataset ]] && echo "$dataset already exists" && exit 1 

cp -r a2d2 $dataset

## Rename yaml files and replace dataset name
file_list=$(find ${dataset}/ -name "*.*" )
for path in $file_list
do
if [[ -f $path ]]
then
    new_path=${path//a2d2/${dataset}}
    new_dir=$( dirname $new_path )
    [[ ! -d $new_dir ]] && mkdir -p $new_dir
    [[ $path != $new_path ]] && echo "Renaming $path -> $new_path" && mv $path $new_path
    echo "Replacing a2d2 -> ${dataset} in $path" && sed -i -e s/a2d2/${dataset}/g $new_path
fi
done

echo "Copy $scripts_dir/a2d2-setup-dataset.sh -> $scripts_dir/${dataset}-setup-dataset.sh"
cp $scripts_dir/a2d2-setup-dataset.sh  $scripts_dir/${dataset}-setup-dataset.sh
echo "Replacing a2d2 -> ${dataset} in $scripts_dir/${dataset}-setup-dataset.sh"
sed -i -e s/a2d2/${dataset}/g $scripts_dir/${dataset}-setup-dataset.sh

echo "Copy $DIR/adds/src/a2d2_ros_util.py ->  $DIR/adds/src/${dataset}_ros_util.py"
cp $DIR/adds/src/a2d2_ros_util.py  $DIR/adds/src/${dataset}_ros_util.py
echo "Replacing a2d2 -> ${dataset} in $DIR/adds/src/${dataset}_ros_util.py"
sed -i -e s/a2d2/${dataset}/g $DIR/adds/src/${dataset}_ros_util.py
