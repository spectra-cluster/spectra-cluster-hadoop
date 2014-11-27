package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IncrementalClusteringEngineFactory;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.IKeyable;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Rui Wang
 * @version $Id$
 */
public abstract class AbstractClusterReducer extends Reducer<Text, Text, Text, Text> {
    private final IncrementalClusteringEngineFactory engineFactory = new IncrementalClusteringEngineFactory();
    private IIncrementalClusteringEngine engine;
    private double clusterRetainThreshold = Defaults.getRetainThreshold();

    private final Set<String> writtenSpectra = new HashSet<String>();
    private final Set<String> lastWrittenSpectra = new HashSet<String>();

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        updateEngine(context, null);
        super.cleanup(context);
    }

    protected abstract <T> void updateEngine(final Context context, final T key) throws IOException, InterruptedException;


    /**
     * Write out a collection of clusters
     */
    protected void writeClusters(Context context, Collection<ICluster> clusters) throws IOException, InterruptedException {
        for (ICluster cluster : clusters) {
            writeCluster(context, cluster);
        }
    }

    /**
     * Before writing out a cluster, remove all the non-fitting spectra and each out as a single-spectrum cluster
     */
    protected void writeCluster(Context context, ICluster cluster) throws IOException, InterruptedException {
        final List<ICluster> allClusters = ClusterUtilities.findNoneFittingSpectra(cluster,
                getEngine().getSimilarityChecker(), getClusterRetainThreshold());

        if (!allClusters.isEmpty()) {

            for (ICluster removedCluster : allClusters) {

                // drop all spectra
                List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
                ISpectrum[] allRemoved = clusteredSpectra.toArray(new ISpectrum[clusteredSpectra.size()]);
                cluster.removeSpectra(allRemoved);

                // and write as stand alone
                writeOneVettedCluster(context, removedCluster);
            }

        }

        // now write the original
        writeOneVettedCluster(context, cluster);     // nothing removed
    }

    protected void writeOneVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        Set<String> spectralIds = cluster.getSpectralIds();

        if (spectralIds.size() == 1) {
            String id = spectralIds.iterator().next();
            if (writtenSpectra.contains(id) || lastWrittenSpectra.contains(id))
                return; // already written
        }
        writtenSpectra.addAll(spectralIds);

        IKeyable key = makeClusterOutputKey(cluster);

        String clusterText = convertClusterToString(cluster);

        keyOutputText.set(key.toString());
        valueOutputText.set(clusterText);

        if (clusterText.length() > (5 * "BEGIN IONS\n".length())) {
            Counter counter = context.getCounter("Cluster Size", "Written cluster");
            counter.increment(1);

            context.write(keyOutputText, valueOutputText);
        }
    }

    /**
     * Default cluster output key is MZKey, this key is used for the reducer output
     * @param cluster   intput cluster
     * @return  output key
     */
    protected IKeyable makeClusterOutputKey(ICluster cluster) {
        return new MZKey(cluster.getPrecursorMz());
    }

    /**
     * convert a cluster to string for output
     */
    protected String convertClusterToString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }

    public IIncrementalClusteringEngine getEngine() {
        return engine;
    }

    public IncrementalClusteringEngineFactory getEngineFactory() {
        return engineFactory;
    }

    public void setEngine(IIncrementalClusteringEngine engine) {
        this.engine = engine;

        lastWrittenSpectra.clear();
        lastWrittenSpectra.addAll(writtenSpectra); // keep one set
        writtenSpectra.clear(); // ready for next set
    }

    public double getClusterRetainThreshold() {
        return clusterRetainThreshold;
    }

    public void setClusterRetainThreshold(double clusterRetainThreshold) {
        this.clusterRetainThreshold = clusterRetainThreshold;
    }
}
