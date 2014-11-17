package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.AbstractClusterReducer;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;

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
public class MajorPeakReducer extends AbstractClusterReducer {

    private double majorPeak;
    private final double majorPeakWindowSize = ClusterHadoopDefaults.getMajorPeakMZWindowSize();
    private final Set<String> writtenSpectra = new HashSet<String>();
    //todo: why do we need last written spectra?
    private final Set<String> lastWrittenSpectra = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());
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
            final Collection<ICluster> removedClusters = getEngine().addClusterIncremental(cluster);

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
    protected <T> void updateEngine(Context context, T peakMZKey) throws IOException, InterruptedException {

        // if the current cluster engine is not null, write out all the existing clusters
        if (getEngine() != null) {
            final Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
            setEngine(null);
        }

        // set new cluster engine and new major peak
        if (peakMZKey != null) {
            // if not at end make a new engine
            setEngine(getEngineFactory().getIncrementalClusteringEngine((float) majorPeakWindowSize));
            setMajorPeak(((PeakMZKey)peakMZKey).getPeakMZ());
        }
    }

    /**
     * this version of writeCluster does all the real work
     */
    protected void writeOneVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();

        if (clusteredSpectra.size() == 1) {
            String id = clusteredSpectra.get(0).getId();
            if (writtenSpectra.contains(id) || lastWrittenSpectra.contains(id))
                return; // already written
            writtenSpectra.add(id);
        }

        super.writeOneVettedCluster(context, cluster);
    }


    @Override
    public void setEngine(IIncrementalClusteringEngine clusterEngine) {
        super.setEngine(clusterEngine);

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
