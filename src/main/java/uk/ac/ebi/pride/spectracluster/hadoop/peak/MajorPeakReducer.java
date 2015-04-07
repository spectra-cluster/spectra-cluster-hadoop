package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.AbstractIncrementalClusterReducer;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;

import java.io.IOException;
import java.util.Collection;

/**
 * Reducer to cluster spectra share the same major peaks
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakReducer extends AbstractIncrementalClusterReducer {

    private double majorPeak;
    private final double majorPeakWindowSize = ClusterHadoopDefaults.getMajorPeakMZWindowSize();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numOfValues = 0;

        PeakMZKey peakMZKey = new PeakMZKey(key.toString());

        // if we are in a different bin - different peak
        if (peakMZKey.getPeakMZ() != getMajorPeak()) {
            updateEngine(context, peakMZKey);
        }

        // iterate and cluster all the spectra
        for (Text val : values) {
            // increment number of values
            ++numOfValues;

            // report progress to avoid timeout
            if ((numOfValues % 50) == 0)
                context.progress();

            final ICluster cluster = IOUtilities.parseClusterFromCGFString(val.toString());

            // incrementally cluster
            final Collection<ICluster> removedClusters = getEngine().addClusterIncremental(cluster);

            // output clusters
            writeClusters(context, removedClusters);
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
        }

        // set new cluster engine and new major peak
        if (peakMZKey != null) {
            // if not at end make a new engine
            setEngine(getEngineFactory().getIncrementalClusteringEngine((float) majorPeakWindowSize));
            setMajorPeak(((PeakMZKey) peakMZKey).getPeakMZ());
        } else {
            setEngine(null);
        }
    }

    public double getMajorPeak() {
        return majorPeak;
    }

    public void setMajorPeak(double majorPeak) {
        this.majorPeak = majorPeak;
    }
}
