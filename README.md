# spectra-cluster-hadoop

# Introduction
The spectra-cluster-hadoop application is the [Apache Hadoop](http://hadoop.apache.org/) of the newly developed [PRIDE Cluster](https://www.ebi.ac.uk/pride/cluster) algorithm. It is used to cluster the complete public data in the [PRIDE](https://www.ebi.ac.uk/pride) repository for MS/MS based proteomics data.

The spectra-cluster-hadoop application relies on the [spectra-cluster](https://github.com/spectra-cluster/spectra-cluster) clustering API. All implementations of relevant clustering algorithms can be found there.

The following descriptions are only based on the clustering pipeline used to create the [PRIDE Cluster](https://www.ebi.ac.uk/pride/cluster) resource.

## The PRIDE Cluster Pipeline
The pipeline itself is run through the [runClusteringJob.sh](https://github.com/spectra-cluster/spectra-cluster-hadoop/blob/master/script/runClusteringJob.sh) script. The order of the lanched jobs and used thresholds are set there. Additionally, detailed configuration options for all jobs can be found in the respective [configuration files](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/conf/job).

The pipeline used to create the [PRIDE Cluster](https://www.ebi.ac.uk/pride/cluster) resource consists of four [MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) jobs:

1. The [Spectrum](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/src/main/java/uk/ac/ebi/pride/spectracluster/hadoop/spectrum) job is used to load all spectra from the source (MGF) files. Spectra are normalized and only the 70 highest peaks per spectrum retained.
2. The [Peak](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/src/main/java/uk/ac/ebi/pride/spectracluster/hadoop/peak) job is used to create the original clustering. The [runClusteringJob.sh](https://github.com/spectra-cluster/spectra-cluster-hadoop/blob/  master/script/runClusteringJob.sh) script launches this job with decreasing thresholds (default starting at an estimated cluster purity of 0.999 in 4 decreasing rounds to a final purity of 0.99). In the first round only spectra that share one of the six highest peaks are compared. In subsequent rounds only spectra that were within the 30 highest scoring matches in the previous round are being compared.
3. The [Merge](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/src/main/java/uk/ac/ebi/pride/spectracluster/hadoop/merge) job merges clusters in neighbouring windows, again with decreasing accuracy (same settings as for [Peak](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/src/main/java/uk/ac/ebi/pride/            spectracluster/hadoop/peak) job used).
4. The [Output](https://github.com/spectra-cluster/spectra-cluster-hadoop/tree/master/src/main/java/uk/ac/ebi/pride/spectracluster/hadoop/output) job writes the result into the .clustering format (see the [clustering-file-reader](https://github.com/spectra-cluster/clustering-file-reader) API for more information).

# Getting started

### Installation
You will need to have [Maven](http://maven.apache.org/) installed in order to build spectra-cluster-hadoop.

```bash
$ mvn clean package
```
After the build, you should find spectra-cluster-hadoop-X.X.X.zip in the target folder.

### Running the library

Unzip the release of the library under a dedicated folder and execute the following command.

```bash
Usage: ./runClusteringJob.sh [main directory] [job prefix = ''] [similarity threshold settings = 0.999:0.99:4] [output folder = main directory]
  [main directory]      Path on Hadoop to use as a working directory. The sub-
                        directory 'spectra' will be used as input directory
  [job prefix]          (optional) A prefix to add to the Hadoop job names.
  [similarity threshold]          (optional) The similarity threshold settings,
                        in the format of <highest threshold>:<final threshold>:<number of steps>
  [output folder]       (optional) If this option is set, the results are
                        written to this folder instead of the [main directory]
```

# Getting help
If you have questions or need additional help, please contact the PRIDE help desk at the EBI.
email: pride-support at ebi.ac.uk (replace at with @).

# Giving your feedback
Please give us your feedback, including error reports, suggestions on improvements, new feature requests. You can do so by opening a new issue at our [issues section](https://github.com/spectra-cluster/spectra-cluster-hadoop/issues)

# How to cite
Please cite this library using one of the following publications:
- Griss J, Foster JM, Hermjakob H, Vizcaíno JA. PRIDE Cluster: building the consensus of proteomics data. Nature methods. 2013;10(2):95-96. doi:10.1038/nmeth.2343. [PDF](http://www.nature.com/nmeth/journal/v10/n2/pdf/nmeth.2343.pdf),  [HTML](http://www.nature.com/nmeth/journal/v10/n2/full/nmeth.2343.html),  [PubMed](http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3667236/)

# Contribute
We welcome all contributions submitted as [pull](https://help.github.com/articles/using-pull-requests/) request.

# License
This project is available under the [Apache 2](http://www.apache.org/licenses/LICENSE-2.0.html) open source software (OSS) license.
