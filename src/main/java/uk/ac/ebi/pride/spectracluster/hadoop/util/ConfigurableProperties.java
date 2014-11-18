package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.NumberUtilities;

import java.io.IOException;

/**
 *
 * Varies configurations for running the clustering
 *
 * To enable these configurations, change the related job xml configuration file.
 *
 * For example, if you want to change LARGE_BINNING_REGION_PROPERTY to 3, you can add the following xml elements
 *
 * <property>
 *  <name>pride.cluster.large.binning.region</name>
 *  <value>3</value>
 * </property>
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class ConfigurableProperties {

    public static final String NUMBER_COMPARED_PEAKS_PROPERTY = "pride.cluster.number.compared.peaks";
    public static final String SIMILARITY_MZ_RANGE_PROPERTY = "pride.cluster.similarity.mz.range";
    public static final String RETAIN_THRESHOLD_PROPERTY = "pride.cluster.retain.threshold";
    public static final String SIMILARITY_THRESHOLD_PROPERTY = "pride.cluster.similarity.threshold";
    public static final String SPECTRUM_MERGE_WINDOW_PROPERTY = "pride.cluster.spectrum.merge.window";
    public static final String MAJOR_PEAK_WINDOW_PROPERTY = "pride.cluster.major.peak.window";


    /**
     * this method and the one below
     *
     * @param configuration source of parameters
     */
    public static void configureAnalysisParameters(Configuration configuration) {
        Defaults.setNumberComparedPeaks(configuration.getInt(NUMBER_COMPARED_PEAKS_PROPERTY, Defaults.DEFAULT_NUMBER_COMPARED_PEAKS));
        Defaults.setSimilarityMZRange(configuration.getFloat(SIMILARITY_MZ_RANGE_PROPERTY, new Float(Defaults.DEFAULT_MZ_RANGE)));
        Defaults.setRetainThreshold(configuration.getFloat(RETAIN_THRESHOLD_PROPERTY, new Float(Defaults.DEFAULT_RETAIN_THRESHOLD)));
        Defaults.setSimilarityThreshold(configuration.getFloat(SIMILARITY_THRESHOLD_PROPERTY, new Float(Defaults.DEFAULT_SIMILARITY_THRESHOLD)));

        // hadoop related properties
        ClusterHadoopDefaults.setMajorPeakMZWindowSize(configuration.getFloat(MAJOR_PEAK_WINDOW_PROPERTY, new Float(ClusterHadoopDefaults.DEFAULT_MAJOR_PEAK_MZ_WINDOW)));
        ClusterHadoopDefaults.setSpectrumMergeMZWindowSize(configuration.getFloat(SPECTRUM_MERGE_WINDOW_PROPERTY, new Float(ClusterHadoopDefaults.DEFAULT_SPECTRUM_MERGE_WINDOW)));
    }


    /**
     * used to write parameters in to a data sink like a clustering file
     *
     * @param out output
     */
    public static void appendAnalysisParameters(Appendable out) {
        try {
            out.append(NUMBER_COMPARED_PEAKS_PROPERTY).append("=").append(String.valueOf(Defaults.getNumberComparedPeaks())).append("\n");
            out.append(SIMILARITY_MZ_RANGE_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getSimilarityMZRange(), 3)).append("\n");
            out.append(SIMILARITY_THRESHOLD_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getSimilarityThreshold(), 3)).append("\n");
            out.append(RETAIN_THRESHOLD_PROPERTY).append("=").append(NumberUtilities.formatDouble(Defaults.getRetainThreshold(), 3)).append("\n");
            out.append(MAJOR_PEAK_WINDOW_PROPERTY).append("=").append(NumberUtilities.formatDouble(ClusterHadoopDefaults.getMajorPeakMZWindowSize(), 3)).append("\n");
            out.append(SPECTRUM_MERGE_WINDOW_PROPERTY).append("=").append(NumberUtilities.formatDouble(ClusterHadoopDefaults.getSpectrumMergeMZWindowSize(), 3)).append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
