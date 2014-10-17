package uk.ac.ebi.pride.spectracluster.hadoop.mgf;

import uk.ac.ebi.pride.spectracluster.hadoop.ClusterLauncher;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class MgfSpectrumDatastoreLauncherMain {

    public static void main(String[] args) {
        ClusterLauncher.workingMain(args, MgfSpectrumDatastoreLancherJobBuilder.FACTORY);
    }
}
