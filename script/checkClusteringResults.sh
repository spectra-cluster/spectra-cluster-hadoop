#!/bin/bash

# This script is used for checking .clustering files end with correct
# ending

# directory where the .clustering files are stored (required)
DIR=$1

for file in ${DIR}/*.clustering
do
        tail -1 $file | grep -q '^SPEC' || echo $file
done