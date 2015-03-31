package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.IncrementalClusteringEngineFactory;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

import java.io.IOException;
import java.util.List;

/**
 * @author Rui Wang
 * @version $Id$
 */
public abstract class AbstractIncrementalClusterReducer extends FilterSingleSpectrumClusterReducer {
    private final IncrementalClusteringEngineFactory engineFactory = new IncrementalClusteringEngineFactory();
    private IIncrementalClusteringEngine engine;
    private double clusterRetainThreshold = Defaults.getRetainThreshold();

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
                super.writeCluster(context, removedCluster);
            }

        }

        // now write the original
        super.writeCluster(context, cluster);     // nothing removed
    }

    public IIncrementalClusteringEngine getEngine() {
        return engine;
    }

    public IncrementalClusteringEngineFactory getEngineFactory() {
        return engineFactory;
    }

    public void setEngine(IIncrementalClusteringEngine engine) {
        this.engine = engine;
        updateCache();
    }

    public double getClusterRetainThreshold() {
        return clusterRetainThreshold;
    }

    public void setClusterRetainThreshold(double clusterRetainThreshold) {
        this.clusterRetainThreshold = clusterRetainThreshold;
    }
}
