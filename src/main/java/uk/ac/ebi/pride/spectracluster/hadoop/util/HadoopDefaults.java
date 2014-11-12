package uk.ac.ebi.pride.spectracluster.hadoop.util;


import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

/**
 * @author Steve Lewis
 * @version $Id$
 */
public final class HadoopDefaults {

    // value can be set using uk.ac.ebi.pride.spectracluster.hadoop.MajorPeakReducer.MajorPeakWindow
    public static final double DEFAULT_MAJOR_PEAK_MZ_WINDOW = 2.0; // major peak sliding window is this

    public static final double DEFAULT_SPECTRUM_MERGE_WINDOW = 0.5;

    public static final double DEFAULT_SAME_CLUSTER_MERGE_WINDOW = DEFAULT_SPECTRUM_MERGE_WINDOW;

    public static final String DEFAULT_OUTPUT_PATH = "ConsolidatedClusters";

    private static String gOutputPath = DEFAULT_OUTPUT_PATH;

    private static double spectrumMergeMZWindowSize = DEFAULT_SPECTRUM_MERGE_WINDOW;

    private static double majorPeakMZWindowSize = DEFAULT_MAJOR_PEAK_MZ_WINDOW;

    private static double sameClusterMergeMZWindowSize = DEFAULT_SAME_CLUSTER_MERGE_WINDOW;

    public static double getSpectrumMergeMZWindowSize() {
        return spectrumMergeMZWindowSize;
    }

    public static double getMajorPeakMZWindowSize() {
        return majorPeakMZWindowSize;
    }

    public static double getSameClusterMergeMZWindowSize() {
        return sameClusterMergeMZWindowSize;
    }

    public static void setOutputPath(String gOutputPath) {
        HadoopDefaults.gOutputPath = gOutputPath;
    }

    public static void setSpectrumMergeMZWindowSize(double spectrumMergeMZWindowSize) {
        HadoopDefaults.spectrumMergeMZWindowSize = spectrumMergeMZWindowSize;
    }

    public static void setMajorPeakMZWindowSize(double majorPeakMZWindowSize) {
        HadoopDefaults.majorPeakMZWindowSize = majorPeakMZWindowSize;
    }

    public static void setSameClusterMergeMZWindowSize(double sameClusterMergeMZWindowSize) {
        HadoopDefaults.sameClusterMergeMZWindowSize = sameClusterMergeMZWindowSize;
    }

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


    @SuppressWarnings("UnusedDeclaration")
    private static final IWideBinner WIDE_MZ_BINNER = new SizedWideBinner(
            MZIntensityUtilities.HIGHEST_USABLE_MZ,
            WIDE_BIN_WIDTH,
            MZIntensityUtilities.LOWEST_USABLE_MZ,
            WIDE_BIN_OVERLAP);


    public static final IWideBinner DEFAULT_WIDE_MZ_BINNER = NARROW_MZ_BINNER;

    public static final HadoopDefaults INSTANCE = new HadoopDefaults();


    private HadoopDefaults() {
    }

    public static String getOutputPath() {
        return gOutputPath;
    }
}
