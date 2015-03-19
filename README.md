# spectra-cluster-hadoop

# Introduction
spectra-cluster-hadoop is a MS spectrum clustering pipeline on [Apache Hadoop](http://hadoop.apache.org/) platform.
The pipeline consists of four [MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) jobs:

1. Initial clustering job using highest peak clustering, assuming clustered spectra will share at least one of their most intense 6 peaks.
2. Merge clusters according to their precursor m/z windows.
3. Merge clusters again with a larger precursor m/z window.
4. Output clusters into clustering result files.

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
$ ./runClusteringJob.sh [main directory on HDFS] [job prefix] [output folder on HDFS]
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
