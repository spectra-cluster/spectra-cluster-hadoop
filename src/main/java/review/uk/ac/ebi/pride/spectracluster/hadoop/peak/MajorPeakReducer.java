package review.uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import review.uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import review.uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import review.uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopDefaults;
import review.uk.ac.ebi.pride.spectracluster.hadoop.util.SpectraHadoopUtilities;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IncrementalClusteringEngineFactory;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reducer to cluster spectra share the same major peaks
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakReducer extends Reducer<Text, Text, Text, Text> {

    private IIncrementalClusteringEngine clusterEngine;
    private double majorPeak;
    private final IncrementalClusteringEngineFactory factory = new IncrementalClusteringEngineFactory();
    private final double majorPeakWindowSize = HadoopDefaults.getMajorPeakMZWindowSize();
    private final double clusterRetainThreshold = Defaults.getRetainThreshold();
    private final Set<String> writtenSpectra = new HashSet<String>();
    //todo: why do we need last written spectra?
    private final Set<String> lastWrittenSpectra = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //todo: leave out all the configuration stuff, as they are using the default anyway
        //todo: if we are not going to use the configuration, we can remove this method in the future
//        ConfigurableProperties.configureAnalysisParameters(getApplication());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        PeakMZKey peakMZKey = new PeakMZKey(key.toString());

        // if we are in a different bin - different peak
        if (peakMZKey.getPeakMZ() != getMajorPeak()) {
            updateEngine(context, peakMZKey);
        }

        // iterate and cluster all the spectra
        for (Text val : values) {
            // convert spectra to clusters
            final ISpectrum match = parseSpectrumFromString(val.toString());
            //todo: why do we need the following commented out line
//            if (match == null)
//                continue; // not sure why this happens but nothing seems like the thing to do
            final ICluster cluster = ClusterUtilities.asCluster(match);

            // incrementally cluster
            final Collection<ICluster> removedClusters = getClusterEngine().addClusterIncremental(cluster);

            // output clusters
            writeClusters(context, removedClusters);
        }
    }

    /**
     * Parse a given string into a spectrum
     *
     * @param originalContent given string content
     * @return parsed spectrum
     */
    private ISpectrum parseSpectrumFromString(String originalContent) {
        LineNumberReader reader = new LineNumberReader(new StringReader(originalContent));

        try {
            return ParserUtilities.readMGFScan(reader);
        } catch (Exception e) {
            throw new IllegalStateException("Error while parsing spectrum", e);
        }
    }

    /**
     * Update the current engine when the major peak m/z value changes
     */
    private void updateEngine(Context context, PeakMZKey peakMZKey) throws IOException, InterruptedException {

        // if the current cluster engine is not null, write out all the existing clusters
        if (getClusterEngine() != null) {
            final Collection<ICluster> clusters = getClusterEngine().getClusters();
            writeClusters(context, clusters);
            setClusterEngine(null);
        }

        // set new cluster engine and new major peak
        if (peakMZKey != null) {
            // if not at end make a new engine
            setClusterEngine(factory.getIncrementalClusteringEngine((float) majorPeakWindowSize));
            setMajorPeak(peakMZKey.getPeakMZ());
        }
    }

    /**
     * Write out a collection of clusters
     */
    private void writeClusters(Context context, Collection<ICluster> clusters) throws IOException, InterruptedException {
        for (ICluster cluster : clusters) {
            writeCluster(context, cluster);
        }
    }

    /**
     * Before writing out a cluster, remove all the non-fitting spectra and each out as a single-spectrum cluster
     */
    private void writeCluster(Context context, ICluster cluster) throws IOException, InterruptedException {
        final List<ICluster> allClusters = ClusterUtilities.findNoneFittingSpectra(cluster,
                getClusterEngine().getSimilarityChecker(), clusterRetainThreshold);

        if (!allClusters.isEmpty()) {

            for (ICluster removedCluster : allClusters) {

                // drop all spectra
                final List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
                ISpectrum[] allRemoved = clusteredSpectra.toArray(new ISpectrum[clusteredSpectra.size()]);
                cluster.removeSpectra(allRemoved);

                // and write as stand alone
                writeOneCluster(context, removedCluster);
            }

        }

        // now write the original
        writeOneCluster(context, cluster);     // nothing removed
    }

    /**
     * Write out a cluster, if clustered spectra is only one, check whether it has already been written before
     */
    private void writeOneCluster(Context context, ICluster cluster) throws IOException, InterruptedException {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();

        if (clusteredSpectra.size() == 1) {
            String id = clusteredSpectra.get(0).getId();
            if (writtenSpectra.contains(id) || lastWrittenSpectra.contains(id))
                return; // already written
            writtenSpectra.add(id);
        }

        writeOneVettedCluster(context, cluster);     // nothing removed
    }

    /**
     * this version of writeCluster does all the real work
     */
    protected void writeOneVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        MZKey key = new MZKey(cluster.getPrecursorMz());

        String clusterText = convertClusterToString(cluster);

        context.write(new Text(key.toString()), new Text(clusterText));

        incrementBinCounters(key, context); // how big are the bins - used in next job
    }

    /**
     * convert a cluster to string for output
     */
    private String convertClusterToString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }


    /**
     * Increment bin counters which will be used by the follow-on jobs
     */
    private void incrementBinCounters(MZKey mzKey, Context context) {
        IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;
        int[] bins = binner.asBins(mzKey.getPrecursorMZ());

        for (int bin : bins) {
            SpectraHadoopUtilities.incrementPartitionCounter(context, "Bin", bin);
        }
    }

    public IIncrementalClusteringEngine getClusterEngine() {
        return clusterEngine;
    }

    public void setClusterEngine(IIncrementalClusteringEngine clusterEngine) {
        this.clusterEngine = clusterEngine;

        lastWrittenSpectra.clear();
        lastWrittenSpectra.addAll(writtenSpectra); // keep one set
        writtenSpectra.clear(); // ready for next set
    }

    public double getMajorPeak() {
        return majorPeak;
    }

    public void setMajorPeak(double majorPeak) {
        this.majorPeak = majorPeak;
    }
}
