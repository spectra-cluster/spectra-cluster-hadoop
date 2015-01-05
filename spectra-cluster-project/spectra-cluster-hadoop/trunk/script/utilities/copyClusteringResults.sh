#!/bin/bash

# This script for copying clustering results from Hadoop cluster
# to local file system

# NOTE: this will run using nohup in the background

# remote directory on Hadoop HDFS (required)
HADOOP_RESULT_DIR=$1
# local directory to store the result files
OUTPUT_LOCAL_DIR=$2

nohup hadoop fs -get ${HADOOP_RESULT_DIR} ${OUTPUT_LOCAL_DIR} &
