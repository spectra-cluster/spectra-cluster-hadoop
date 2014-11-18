package uk.ac.ebi.pride.spectracluster.hadoop.util;


import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

/**
 * Default configurations for running clustering on Hadoop cluster
 *
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public final class ClusterHadoopDefaults {

    public static final double DEFAULT_MAJOR_PEAK_MZ_WINDOW = 2.0; // major peak sliding window is this

    public static final double DEFAULT_SPECTRUM_MERGE_WINDOW = 0.5;

    public static final String DEFAULT_BINNING_RESOURCE = "/pride-binning.tsv";

    /**
     * binning sizes
     */
    private static final double NARRROW_BIN_WIDTH = 0.6; // 0.15; //0.005; // 0.3;
    private static final double NARRROW_BIN_OVERLAP = 0.15; // 0.03; //0.002; // 0.1;

    public static final IWideBinner DEFAULT_WIDE_MZ_BINNER = new SizedWideBinner(
            MZIntensityUtilities.HIGHEST_USABLE_MZ,
            NARRROW_BIN_WIDTH,
            MZIntensityUtilities.LOWEST_USABLE_MZ,
            NARRROW_BIN_OVERLAP);


    private static double spectrumMergeMZWindowSize = DEFAULT_SPECTRUM_MERGE_WINDOW;

    private static double majorPeakMZWindowSize = DEFAULT_MAJOR_PEAK_MZ_WINDOW;

    private static String binningResource = DEFAULT_BINNING_RESOURCE;

    private static IWideBinner binner = DEFAULT_WIDE_MZ_BINNER;

    private ClusterHadoopDefaults() {
    }

    public static double getSpectrumMergeMZWindowSize() {
        return spectrumMergeMZWindowSize;
    }

    public static double getMajorPeakMZWindowSize() {
        return majorPeakMZWindowSize;
    }

    public static String getBinningResource() {
        return binningResource;
    }

    public static IWideBinner getBinner() {
        return binner;
    }

    public static void setBinner(IWideBinner binner) {
        ClusterHadoopDefaults.binner = binner;
    }

    public static void setBinningResource(String binningResource) {
        ClusterHadoopDefaults.binningResource = binningResource;
    }

    public static void setSpectrumMergeMZWindowSize(double spectrumMergeMZWindowSize) {
        ClusterHadoopDefaults.spectrumMergeMZWindowSize = spectrumMergeMZWindowSize;
    }

    public static void setMajorPeakMZWindowSize(double majorPeakMZWindowSize) {
        ClusterHadoopDefaults.majorPeakMZWindowSize = majorPeakMZWindowSize;
    }
}
