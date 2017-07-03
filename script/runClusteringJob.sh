#!/bin/sh

# input options (required)
ROOT_DIR=$1

# make sure the parameter was set
if [ -z "$ROOT_DIR" ]; then
    echo "Usage: $0 [main directory] [job prefix = ''] [similarity threshold settings = 0.999:0.99:4] [output folder = main directory]"
    echo "  [main directory]      Path on Hadoop to use as a working directory. The sub-"
    echo "                        directory 'spectra' will be used as input directory"
    echo "  [job prefix]          (optional) A prefix to add to the Hadoop job names."
    echo "  [similarity threshold]          (optional) The similarity threshold settings,"
    echo "                        in the format of <highest threshold>:<final threshold>:<number of steps>"
    echo "  [output folder]       (optional) If this option is set, the results are"
    echo "                        written to this folder instead of the [main directory]"
    exit 1
fi

# job prefix (optional)
JOB_PREFIX=""
if [ "$2" != "" ]; then
    JOB_PREFIX="_$2"
fi

# similarity threshold settings (optional)
UPPER_SIMILARITY_THRESHOLD="0.999"
FINAL_SIMILARITY_THRESHOLD="0.99"
NUMBER_OF_SIMILARITY_STEPS="4"
DO_MERGING="1"

INITIAL_WINDOW_SIZE="0.5"
FOLLOWING_WINDOW_SIZE="4" # This option only affects the mapper but not the clustering's precursor tolerance. The
                          # precursor tolerance is defined in the job's XML config file.
SUBSEQUENT_ROUND="0" # set to 0 to use sharing of major peaks with larger window size again

if [ -n "$3" ]; then
    SIMILARITY_SETTINGS=(${3//:/ })
    UPPER_SIMILARITY_THRESHOLD="${SIMILARITY_SETTINGS[0]}"
    FINAL_SIMILARITY_THRESHOLD="${SIMILARITY_SETTINGS[1]}"
    NUMBER_OF_SIMILARITY_STEPS="${SIMILARITY_SETTINGS[2]}"
fi

# calculate the similarity step size
SIMILARITY_STEP_SIZE=$( bc <<< "scale=7; (${UPPER_SIMILARITY_THRESHOLD} - ${FINAL_SIMILARITY_THRESHOLD}) / ${NUMBER_OF_SIMILARITY_STEPS}" )

# Create an array of similarity thresholds in descending order
SIMILARITY_THRESHOLDS=("${UPPER_SIMILARITY_THRESHOLD}")
CURRENT_THRESHOLD="${UPPER_SIMILARITY_THRESHOLD}"
for i in $(seq 1 ${NUMBER_OF_SIMILARITY_STEPS});
do
    CURRENT_THRESHOLD=$(bc <<< "${CURRENT_THRESHOLD}-${SIMILARITY_STEP_SIZE}")
    SIMILARITY_THRESHOLDS+=("${CURRENT_THRESHOLD}")
done

# job output directory
OUTPUT_ROOT="$ROOT_DIR"
if [ -n "$4" ]; then
    OUTPUT_ROOT="$4"
fi

# inferred input directory and output directory
# here we assume that there is a spectra directory which contains all the spectra and
# the final results will be outputted to clustering_results folder
INPUT_DIR="${ROOT_DIR}/spectra"
OUTPUT_DIR="${OUTPUT_ROOT}/clustering_results"

# intermediate directorys for storing intermediate results from different steps
SPECTRUM_TO_CLUSTER_DIR="${OUTPUT_ROOT}/spectrum_to_cluster"
MAJOR_PEAK_DIR="${OUTPUT_ROOT}/major_peak"
MERGE_DIR="${OUTPUT_ROOT}/merge"

# counter files, used by each job to store the numbers in counters
SPECTRUM_TO_CLUSTER_COUNTER_FILE="${OUTPUT_ROOT}/spectrum_to_cluster.counter"
MAJOR_PEAK_COUNTER_FILE="${OUTPUT_ROOT}/major_peak.counter"
MERGE_COUNTER_FILE="${OUTPUT_ROOT}/merge.counter"
OUTPUT_COUNTER_FILE="${OUTPUT_ROOT}/output.counter"

# General hadoop configuration
#HADOOP_CONF=conf/hadoop/hadoop-prod-cluster.xml
#HADOOP_CONF=conf/hadoop/hadoop-dev-cluster.xml
#HADOOP_CONF=conf/hadoop/hadoop-local.xml
HADOOP_CONF=conf/hadoop/hadoop-prod-new-cluster.xml

# Path to configuration files for each job
# NOTE: conf is on the classpath, so there is not need for specific the full path
JOB_CONF=job

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
hadoop fs -conf ${HADOOP_CONF} -rm -r ${SPECTRUM_TO_CLUSTER_DIR}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_DIR}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${MERGE_DIR}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${OUTPUT_DIR}

# remove existing counter files
hadoop fs -conf ${HADOOP_CONF} -rm -r ${SPECTRUM_TO_CLUSTER_COUNTER_FILE}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_COUNTER_FILE}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${MERGE_COUNTER_FILE}
hadoop fs -conf ${HADOOP_CONF} -rm -r ${OUTPUT_COUNTER_FILE}

# execute the spectrum to cluster job
echo "Start executing the spectrum to cluster job"

hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.spectrum.SpectrumToClusterJob -libjars ${LIB_JARS} -conf ${HADOOP_CONF} "SPECTRUM_TO_CLUSTER${JOB_PREFIX}" "${JOB_CONF}/spectrum-to-cluster.xml" ${SPECTRUM_TO_CLUSTER_COUNTER_FILE} ${SPECTRUM_TO_CLUSTER_DIR} ${INPUT_DIR} ${FOLLOWING_WINDOW_SIZE}

# check exit code of the spectrum to cluster job
check_exit_code $? "Failed to finish the spectrum to cluster job" "The spectrum to cluster job has finished successfully"

# execute the major clustering job
echo "Start executing the major clustering job using ${UPPER_SIMILARITY_THRESHOLD} as similarity threshold"

hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.clustering.MajorPeakJob -libjars ${LIB_JARS} -conf ${HADOOP_CONF} "MAJOR_PEAK${JOB_PREFIX}" "${JOB_CONF}/major-peak-first_run.xml" ${MAJOR_PEAK_COUNTER_FILE} ${UPPER_SIMILARITY_THRESHOLD} ${INITIAL_WINDOW_SIZE} 1 ${MAJOR_PEAK_DIR} ${SPECTRUM_TO_CLUSTER_DIR} ${SPECTRUM_TO_CLUSTER_COUNTER_FILE}

# check exit code of the major clustering job
check_exit_code $? "Failed to finish the major clustering job" "The major clustering job has finished successfully"

CURRENT_ROUND="${SUBSEQUENT_ROUND}"

# execute the existing clustering job
for key in ${!SIMILARITY_THRESHOLDS[@]};
do
    if [ "${key}" != "0" ]; then
        echo "Start executing the existing major clustering job using ${SIMILARITY_THRESHOLDS[${key}]} as similarity threshold (round $CURRENT_ROUND)"

        CURRENT_ROUND=$[$CURRENT_ROUND+1]

        # make sure the directories were deleted
        hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_DIR}_last
        hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_COUNTER_FILE}_last

        hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.clustering.MajorPeakJob -libjars ${LIB_JARS} -conf ${HADOOP_CONF} "MAJOR_PEAK${JOB_PREFIX}" "${JOB_CONF}/major-peak.xml" ${MAJOR_PEAK_COUNTER_FILE} ${SIMILARITY_THRESHOLDS[${key}]} ${FOLLOWING_WINDOW_SIZE} ${CURRENT_ROUND} ${MAJOR_PEAK_DIR}_last ${MAJOR_PEAK_DIR} ${SPECTRUM_TO_CLUSTER_COUNTER_FILE}

        # check exit code of the existing clustering job
        check_exit_code $? "Failed to finish the major clustering job" "The major clustering job has finished successfully"

        # delete old results
        hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_DIR}
        hadoop fs -conf ${HADOOP_CONF} -rm -r ${MAJOR_PEAK_COUNTER_FILE}

        # move the new results
        hadoop fs -conf ${HADOOP_CONF} -mv ${MAJOR_PEAK_DIR}_last ${MAJOR_PEAK_DIR}
        hadoop fs -conf ${HADOOP_CONF} -mv ${MAJOR_PEAK_COUNTER_FILE}_last ${MAJOR_PEAK_COUNTER_FILE}
    fi
done


# execute merge cluster by offset job
MERGE_INPUT_DIR="${MAJOR_PEAK_DIR}"
CURRENT_ROUND="${SUBSEQUENT_ROUND}"

# execute the merging job
for key in ${!SIMILARITY_THRESHOLDS[@]};
do
    if [ "${key}" != "0" ]; then
        echo "Start executing the existing major clustering merging job using ${SIMILARITY_THRESHOLDS[${key}]} as similarity threshold (round $CURRENT_ROUND)"

        CURRENT_ROUND=$[$CURRENT_ROUND+1]
        hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.clustering.MajorPeakJob -libjars ${LIB_JARS} -conf ${HADOOP_CONF} "MAJOR_PEAK_MERGE${JOB_PREFIX}" "${JOB_CONF}/major-peak-merging.xml" ${MERGE_COUNTER_FILE} ${SIMILARITY_THRESHOLDS[${key}]} ${FOLLOWING_WINDOW_SIZE} ${CURRENT_ROUND} ${MERGE_DIR} ${MERGE_INPUT_DIR} ${SPECTRUM_TO_CLUSTER_COUNTER_FILE}

        MERGE_INPUT_DIR="${MERGE_DIR}_last"
        hadoop fs -conf ${HADOOP_CONF} -rm -r ${MERGE_INPUT_DIR}
        hadoop fs -conf ${HADOOP_CONF} -mv ${MERGE_DIR} ${MERGE_INPUT_DIR}

        # check exit code for merge cluster by offset job
        check_exit_code $? "Failed to finish the merge cluster by offset job" "The merge cluster by offset job has finished successfully"
    fi
done

# execute output job
echo "Start executing the output job"

hadoop jar ${project.build.finalName}.jar uk.ac.ebi.pride.spectracluster.hadoop.output.OutputClusterJob -libjars ${LIB_JARS} -conf ${HADOOP_CONF} "OUTPUT_CLUSTER${JOB_PREFIX}" "${JOB_CONF}/output-cluster.xml" ${OUTPUT_COUNTER_FILE} ${OUTPUT_DIR} ${MERGE_INPUT_DIR}

# check exit code for the output job
check_exit_code $? "Failed to finish the output cluster job" "The output cluster job has finished successfully"