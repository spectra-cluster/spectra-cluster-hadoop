#!/bin/sh

# input options
ROOT_DIR=$1
# optional job prefix
JOB_PREFIX=""
if [ "$2" != "" ]; then
    JOB_PREFIX="_$2"
fi

# inferred input directory and output directory
# here we assume that there is a spectra directory which contains all the spectra and
# the final results will be outputted to clustering_results folder
INPUT_DIR="${ROOT_DIR}/spectra"
OUTPUT_DIR="${ROOT_DIR}/clustering_results"

# intermediate directorys for storing intermediate results from different steps
MAJOR_PEAK_DIR="${ROOT_DIR}/major_peak"
MERGE_BY_OFFSET_DIR="${ROOT_DIR}/merge_by_offset"
MERGE_DIR="${ROOT_DIR}/merge"

# counter files, used by each job to store the numbers in counters
MAJOR_PEAK_COUNTER_FILE="${ROOT_DIR}/major_peak.counter"
MERGE_BY_OFFSET_COUNTER_FILE="${ROOT_DIR}/merge_by_offset.counter"
MERGE_COUNTER_FILE="${ROOT_DIR}/merge.counter"
OUTPUT_COUNTER_FILE="${ROOT_DIR}/output.counter"

# Path to configuration files for each job
JOB_CONF=conf/job-yarn

# build library jars for hadoop job to move jars into distributed cache
# this is hadoop way of adding dependencies to a cluster
function build_library_jars() {
    LIB_JARS=""

    for f in lib/*.jar
    do
        if [ -z "${LIB_JARS}" ]; then
            LIB_JARS="$f"
        else
            LIB_JARS="${LIB_JARS},$f"
        fi
    done
}

# check exit code, if detect error, then exit the script and print out an error message
# otherwise, print out a success message
function check_exit_code() {
    local exit_code=$1
    local error_message="$2"
    local success_message="$3"

    if [[ $exit_code != 0 ]]; then
        echo $error_message
        exit $exit_code
    else
        echo $success_message
    fi
}

# concatinate the jar libraries into one string
# solution from: http://grepalex.com/2013/02/25/hadoop-libjars/
build_library_jars

# remove the intermediate directories if they exists
hadoop fs -rm -r ${MAJOR_PEAK_DIR}
hadoop fs -rm -r ${MERGE_BY_OFFSET_DIR}
hadoop fs -rm -r ${MERGE_DIR}
hadoop fs -rm -r ${OUTPUT_DIR}

# remove existing counter files
hadoop fs -rm -r ${MAJOR_PEAK_COUNTER_FILE}
hadoop fs -rm -r ${MERGE_BY_OFFSET_COUNTER_FILE}
hadoop fs -rm -r ${MERGE_COUNTER_FILE}
hadoop fs -rm -r ${OUTPUT_COUNTER_FILE}

# execute the major peak job
hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.peak.MajorPeakJob -libjars ${LIB_JARS} ${INPUT_DIR} ${MAJOR_PEAK_DIR} "MAJOR_PEAK${JOB_PREFIX}" "${JOB_CONF}/major-peak.xml" ${MAJOR_PEAK_COUNTER_FILE}

# check exit code of the major peak job
check_exit_code $? "Failed to finish the major peak job" "The major peak job has finished successfully"

# execute merge cluster by offset job
hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.merge.MergeClusterJob -libjars ${LIB_JARS} ${MAJOR_PEAK_DIR} ${MERGE_BY_OFFSET_DIR} "MERGE_CLUSTER_BY_OFFSET${JOB_PREFIX}" "${JOB_CONF}/merge-cluster-by-offset.xml" ${MERGE_BY_OFFSET_COUNTER_FILE}

# check exit code for merge cluster by offset job
check_exit_code $? "Failed to finish the merge cluster by offset job" "The merge cluster by offset job has finished successfully"

# execute merge job
hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.merge.MergeClusterJob -libjars ${LIB_JARS} ${MERGE_BY_OFFSET_DIR} ${MERGE_DIR} "MERGE_CLUSTER${JOB_PREFIX}" "${JOB_CONF}/merge-cluster.xml" ${MERGE_COUNTER_FILE}

# check exit code for merge cluster job
check_exit_code $? "Failed to finish the merge cluster job" "The merge cluster job has finished successfully"

# execute output job
hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.output.OutputClusterJob -libjars ${LIB_JARS} ${MERGE_DIR} ${OUTPUT_DIR} "OUTPUT_CLUSTER${JOB_PREFIX}" "${JOB_CONF}/output-cluster.xml" ${OUTPUT_COUNTER_FILE}

# check exit code for the output job
check_exit_code $? "Failed to finish the output cluster job" "The output cluster job has finished successfully"