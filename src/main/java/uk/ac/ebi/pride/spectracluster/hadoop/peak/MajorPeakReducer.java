package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.AbstractIncrementalClusterReducer;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.predicate.IComparisonPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.IPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.ClusterShareMajorPeakPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster_comparison.IsKnownComparisonsPredicate;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Reducer to cluster spectra share the same major peaks
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakReducer extends AbstractIncrementalClusterReducer {

    private int currentBin;
    private final double majorPeakWindowSize = ClusterHadoopDefaults.getMajorPeakMZWindowSize();
    /**
     * Spectra that were already processed to prevent duplication
     */
    private final Set<String> clusteredSpectraIds = new HashSet<String>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numOfValues  = 0;

        BinMZKey binMZKey = new BinMZKey(key.toString());

        if (binMZKey.getBin() != getCurrentBin()) {
            updateEngine(context, binMZKey);
            context.getCounter("Cluster Size", "Processed bins").increment(1);
        }

        // iterate and cluster all the spectra
        for (Text val : values) {
            // increment number of values
            ++numOfValues;

            // report progress to avoid timeout
            if ((numOfValues % 50) == 0)
                context.progress();

            final ICluster cluster = IOUtilities.parseClusterFromCGFString(val.toString());

            if (clusteredSpectraIds.contains(cluster.getId())) {
                context.getCounter("Cluster Size", "Duplicated input spectra").increment(1);
                continue;
            }
            else
                clusteredSpectraIds.add(cluster.getId());

            // update engine if total number of clusters is above threshold
            if (getEngine().getClusters().size() > ClusterHadoopDefaults.getMaximumNumberOfClusters() && ClusterHadoopDefaults.getMaximumNumberOfClusters() > 0) {
                updateEngine(context, binMZKey);
                context.getCounter("Cluster size", "Engine updates in bin").increment(1);
            }

            // incrementally cluster
            final Collection<ICluster> removedClusters = getEngine().addClusterIncremental(cluster);

            // output clusters
            writeClusters(context, removedClusters);
        }
    }

    /**
     * Update the current engine when the major peak m/z value changes
     */
    protected <T> void updateEngine(Context context, T binMZKey) throws IOException, InterruptedException {

        // if the current cluster engine is not null, write out all the existing clusters
        if (getEngine() != null) {
            final Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
        }

        // set new cluster engine and new current bin
        if (binMZKey != null) {
            int currentRound = context.getConfiguration().getInt(MajorPeakJob.CURRENT_CLUSTERING_ROUND, 1);
            IComparisonPredicate<ICluster> clusteringPredicate;

            if (currentRound == 1)
                clusteringPredicate = new ClusterShareMajorPeakPredicate(Defaults.getMajorPeakCount());
            else
                clusteringPredicate = new IsKnownComparisonsPredicate();

            // if not at end make a new engine
            setEngine(getEngineFactory().buildInstance((float) majorPeakWindowSize, clusteringPredicate));
            setCurrentBin(((BinMZKey) binMZKey).getBin());
        } else {
            setEngine(null);
        }

        clusteredSpectraIds.clear();
    }

    public int getCurrentBin() {
        return currentBin;
    }

    public void setCurrentBin(int bin) {
        this.currentBin = bin;
    }
}
