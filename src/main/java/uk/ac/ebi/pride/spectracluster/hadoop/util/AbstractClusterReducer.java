package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IncrementalClusteringEngineFactory;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Rui Wang
 * @version $Id$
 */
public abstract class AbstractClusterReducer extends Reducer<Text, Text, Text, Text> {

    private final IncrementalClusteringEngineFactory engineFactory = new IncrementalClusteringEngineFactory();
    private IIncrementalClusteringEngine engine;
    private double clusterRetainThreshold = Defaults.getRetainThreshold();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //todo: leave out all the configuration stuff, as they are using the default anyway
        //todo: if we are not going to use the configuration, we can remove this method in the future
//        ConfigurableProperties.configureAnalysisParameters(getApplication());
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
                final List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
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
        MZKey key = new MZKey(cluster.getPrecursorMz());

        String clusterText = convertClusterToString(cluster);

        context.write(new Text(key.toString()), new Text(clusterText));
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
    }

    public double getClusterRetainThreshold() {
        return clusterRetainThreshold;
    }

    public void setClusterRetainThreshold(double clusterRetainThreshold) {
        this.clusterRetainThreshold = clusterRetainThreshold;
    }
}
