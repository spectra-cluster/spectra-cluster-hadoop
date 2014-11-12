package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.NumberUtilities;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 *
 * Note: Rui has moved all the properties used into this class
 *
 * @author Rui Wang
 * @version $Id$
 */
public class ConfigurableProperties {

    public static final String LARGE_BINNING_REGION_PROPERTY = "uk.ac.ebi.pride.spectracluster.similarity.FrankEtAlDotProduct.LargeBinningRegion";
    public static final String NUMBER_COMPARED_PEAKS_PROPERTY = "uk.ac.ebi.pride.spectracluster.similarity.FrankEtAlDotProduct.NumberComparedPeaks";
    public static final String SIMILARITY_MZ_RANGE_PROPERTY = "uk.ac.ebi.pride.spectracluster.similarity.FrankEtAlDotProduct.SimilarityMZRange";
    public static final String RETAIN_THRESHOLD_PROPERTY = "uk.ac.ebi.pride.spectracluster.similarity.FrankEtAlDotProduct.RetainThreshold";
    public static final String SIMILARITY_THRESHOLD_PROPERTY = "uk.ac.ebi.pride.spectracluster.similarity.FrankEtAlDotProduct.SimilarityThreshold";
    public static final String STABLE_CLUSTER_SIZE_PROPERTY = "uk.ac.ebi.pride.spectracluster.util.ClusterUtilities.StableClusterSize";
    public static final String SEMI_STABLE_CLUSTER_SIZE_PROPERTY = "uk.ac.ebi.pride.spectracluster.util.ClusterUtilities.SemiStableClusterSize";
    public static final String SPECTRUM_MERGE_WINDOW_PROPERTY = "uk.ac.ebi.pride.spectracluster.hadoop.SameClustererMerger.SpectrumMergeWindow";
    public static final String MAJOR_PEAK_WINDOW_PROPERTY = "uk.ac.ebi.pride.spectracluster.hadoop.MajorPeakReducer.MajorPeakWindow";
    public static final String OUTPUT_PATH_PROPERTY = "uk.ac.ebi.pride.spectracluster.hadoop.OutputPath";


    /**
     * this method and the one below
     *
     * @param application source of parameters
     */
    public static void configureAnalysisParameters(@Nonnull IParameterHolder application) {
        Defaults.setLargeBinningRegion(application.getIntParameter(LARGE_BINNING_REGION_PROPERTY, Defaults.DEFAULT_LARGE_BINNING_REGION));
        Defaults.setNumberComparedPeaks(application.getIntParameter(NUMBER_COMPARED_PEAKS_PROPERTY, Defaults.DEFAULT_NUMBER_COMPARED_PEAKS));
        Defaults.setSimilarityMZRange(application.getDoubleParameter(SIMILARITY_MZ_RANGE_PROPERTY, Defaults.DEFAULT_MZ_RANGE));
        Defaults.setRetainThreshold(application.getDoubleParameter(RETAIN_THRESHOLD_PROPERTY, Defaults.DEFAULT_RETAIN_THRESHOLD));
        Defaults.setSimilarityThreshold(application.getDoubleParameter(SIMILARITY_THRESHOLD_PROPERTY, Defaults.DEFAULT_SIMILARITY_THRESHOLD));

        // hadoop related properties
        HadoopDefaults.setSameClusterMergeMZWindowSize(application.getDoubleParameter(SPECTRUM_MERGE_WINDOW_PROPERTY, HadoopDefaults.DEFAULT_SAME_CLUSTER_MERGE_WINDOW));
        HadoopDefaults.setMajorPeakMZWindowSize(application.getDoubleParameter(MAJOR_PEAK_WINDOW_PROPERTY, HadoopDefaults.DEFAULT_MAJOR_PEAK_MZ_WINDOW));
        HadoopDefaults.setSpectrumMergeMZWindowSize(application.getDoubleParameter(SPECTRUM_MERGE_WINDOW_PROPERTY, HadoopDefaults.DEFAULT_SPECTRUM_MERGE_WINDOW));

        HadoopDefaults.setOutputPath(application.getParameter(OUTPUT_PATH_PROPERTY, HadoopDefaults.DEFAULT_OUTPUT_PATH));
    }


    /**
     * used to write parameters in to a data sink like a clustering file
     *
     * @param out output
     */
    public static void appendAnalysisParameters(@Nonnull Appendable out) {
        try {
            out.append("largeBinningRegion=").append(String.valueOf(Defaults.getLargeBinningRegion())).append("\n");
            out.append("numberComparedPeaks=").append(String.valueOf(Defaults.getNumberComparedPeaks())).append("\n");
            out.append("similarityMZRange=").append(NumberUtilities.formatDouble(Defaults.getSimilarityMZRange(), 3)).append("\n");
            out.append("similarityThreshold=").append(NumberUtilities.formatDouble(Defaults.getSimilarityThreshold(), 3)).append("\n");
            out.append("retainThreshold=").append(NumberUtilities.formatDouble(Defaults.getRetainThreshold(), 3)).append("\n");
            out.append("sameClusterMergeMZWindowSize=").append(NumberUtilities.formatDouble(HadoopDefaults.getSameClusterMergeMZWindowSize(), 3)).append("\n");
            out.append("majorPeakMZWindowSize=").append(NumberUtilities.formatDouble(HadoopDefaults.getMajorPeakMZWindowSize(), 3)).append("\n");
            out.append("spectrumMergeMZWindowSize=").append(NumberUtilities.formatDouble(HadoopDefaults.getSpectrumMergeMZWindowSize(), 3)).append("\n");
            out.append("outputPath=").append(HadoopDefaults.getOutputPath()).append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
