#!/bin/bash

# This script is used for checking mgf files can be parse by the MGFInputFormat
# which is used by Hadoop job to parse the mgf files

# directory where the mgf files are stored (required)
DIR=$1
MEMORY_LIMIT=2500
BASEDIR=$(dirname $0)

for file in ${DIR}/*.mgf
do
    fname=`basename $file`
    bsub -M ${MEMORY_LIMIT} -R "rusage[mem=${MEMORY_LIMIT}]" -q production-rh6 -o /dev/null -g /check_mgf_file -J MGF_CHECK ${BASEDIR}/runInJava.sh ./log/${fname}.log ${MEMORY_LIMIT}M -cp spectra-cluster-hadoop-1.0.1-SNAPSHOT.jar uk.ac.ebi.pride.spectracluster.hadoop.io.MGFInputFormatRunner $file
done
