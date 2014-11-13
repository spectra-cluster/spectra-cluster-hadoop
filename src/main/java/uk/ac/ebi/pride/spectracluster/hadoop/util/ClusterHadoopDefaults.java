package uk.ac.ebi.pride.spectracluster.hadoop.util;


import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

/**
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public final class ClusterHadoopDefaults {

    // value can be set using uk.ac.ebi.pride.spectracluster.hadoop.MajorPeakReducer.MajorPeakWindow
    public static final double DEFAULT_MAJOR_PEAK_MZ_WINDOW = 2.0; // major peak sliding window is this

    public static final double DEFAULT_SPECTRUM_MERGE_WINDOW = 0.5;

    public static final double DEFAULT_SAME_CLUSTER_MERGE_WINDOW = DEFAULT_SPECTRUM_MERGE_WINDOW;

    public static final String DEFAULT_OUTPUT_PATH = "ConsolidatedClusters";

    public static final String DEFAULT_BINNING_RESOURCE = "pride-binning.tsv";

    private static String gOutputPath = DEFAULT_OUTPUT_PATH;

    private static double spectrumMergeMZWindowSize = DEFAULT_SPECTRUM_MERGE_WINDOW;

    private static double majorPeakMZWindowSize = DEFAULT_MAJOR_PEAK_MZ_WINDOW;

    private static double sameClusterMergeMZWindowSize = DEFAULT_SAME_CLUSTER_MERGE_WINDOW;

    private static String binningResource = DEFAULT_BINNING_RESOURCE;


    /**
     * binning sizes
     */
    private static final double NARRROW_BIN_WIDTH = 0.6; // 0.15; //0.005; // 0.3;
    private static final double NARRROW_BIN_OVERLAP = 0.15; // 0.03; //0.002; // 0.1;

    private static final double WIDE_BIN_WIDTH = 1.0;
    private static final double WIDE_BIN_OVERLAP = 0.3;


    private static final IWideBinner NARROW_MZ_BINNER = new SizedWideBinner(
            MZIntensityUtilities.HIGHEST_USABLE_MZ,
            NARRROW_BIN_WIDTH,
            MZIntensityUtilities.LOWEST_USABLE_MZ,
            NARRROW_BIN_OVERLAP);


    private static final IWideBinner WIDE_MZ_BINNER = new SizedWideBinner(
            MZIntensityUtilities.HIGHEST_USABLE_MZ,
            WIDE_BIN_WIDTH,
            MZIntensityUtilities.LOWEST_USABLE_MZ,
            WIDE_BIN_OVERLAP);


    public static final IWideBinner DEFAULT_WIDE_MZ_BINNER = NARROW_MZ_BINNER;


    private ClusterHadoopDefaults() {
    }


    public static double getSpectrumMergeMZWindowSize() {
        return spectrumMergeMZWindowSize;
    }

    public static double getMajorPeakMZWindowSize() {
        return majorPeakMZWindowSize;
    }

    public static double getSameClusterMergeMZWindowSize() {
        return sameClusterMergeMZWindowSize;
    }

    public static String getBinningResource() {
        return binningResource;
    }

    public static void setBinningResource(String binningResource) {
        ClusterHadoopDefaults.binningResource = binningResource;
    }

    public static String getOutputPath() {
        return gOutputPath;
    }

    public static void setOutputPath(String gOutputPath) {
        ClusterHadoopDefaults.gOutputPath = gOutputPath;
    }

    public static void setSpectrumMergeMZWindowSize(double spectrumMergeMZWindowSize) {
        ClusterHadoopDefaults.spectrumMergeMZWindowSize = spectrumMergeMZWindowSize;
    }

    public static void setMajorPeakMZWindowSize(double majorPeakMZWindowSize) {
        ClusterHadoopDefaults.majorPeakMZWindowSize = majorPeakMZWindowSize;
    }

    public static void setSameClusterMergeMZWindowSize(double sameClusterMergeMZWindowSize) {
        ClusterHadoopDefaults.sameClusterMergeMZWindowSize = sameClusterMergeMZWindowSize;
    }
}
