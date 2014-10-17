package uk.ac.ebi.pride.spectracluster.hadoop;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ClusterSpectraThenExit
 * Runs the Clusterer but when finished shuts down the system
 */
public class ClusterSpectraThenExit {
    public static void main(String[] args) throws Exception{
        // Run the cluster launcher
        ClusterLauncher.main(args);
        // Done we can shut down
        System.exit(0);
      }

}
