package uk.ac.ebi.pride.spectracluster.hadoop.util;

/**
 * Created by jg on 12.06.15.
 */
public final class HadoopClusterProperties {
    private HadoopClusterProperties() {

    }

    /**
     * Bin set by the spectrum to cluster job
     */
    public static final String SPECTRUM_TO_CLUSTER_BIN = "spectrum.cluster.bin";

    public static final String MAJOR_PEAK_CLUSTER_BIN = "major_peak.cluster.bin";

    public static final String BIN_PREFIX = "mzBin_";
}
