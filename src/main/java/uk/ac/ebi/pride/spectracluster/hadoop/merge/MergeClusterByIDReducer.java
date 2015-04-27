package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.util.AbstractClusterReducer;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;

import java.io.IOException;
import java.util.Collection;

/**
 * MergeClusterByIDReducer clusters using ClusteringEngine
 *
 * @author Rui Wang
 * @version $Id$
 */
public class MergeClusterByIDReducer extends AbstractClusterReducer {

    public static final int NUMBER_OF_CLUSTER_ITERATIONS = 3;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numOfValues = 0;

        updateEngine(context, key.toString());

        // iterate and cluster all the spectra
        for (Text val : values) {
            // increment number of values
            ++numOfValues;

            // report progress to avoid timeout
            if ((numOfValues % 50) == 0)
                context.progress();

            // convert clusters
            final ICluster cluster = IOUtilities.parseClusterFromCGFString(val.toString());

            // add to be clustered
            getEngine().addClusters(cluster);
        }

        // clustering
        for (int i = 0; i < NUMBER_OF_CLUSTER_ITERATIONS; i++) {
            boolean changed = getEngine().processClusters();
            if (!changed)
                break;
        }
    }

    /**
     * Update the current engine when the major peak m/z value changes
     */
    protected <T> void updateEngine(Context context, T id) throws IOException, InterruptedException {

        // if the current cluster engine is not null, write out all the existing clusters
        if (getEngine() != null) {
            final Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
        }

        // set new cluster engine and new major peak
        if (id != null) {
            // if not at end make a new engine
            setEngine(getEngineFactory().buildInstance());
        } else {
            setEngine(null);
        }
    }
}
