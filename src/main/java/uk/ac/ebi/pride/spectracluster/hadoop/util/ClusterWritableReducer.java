package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.IKeyable;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;

import java.io.IOException;
import java.util.Collection;

/**
 * ClusterWritableReducer is an abstract class that provide output functionality for cluster
 *
 * @author Rui Wang
 * @version $Id$
 */
public abstract class ClusterWritableReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    /**
     * Write out a collection of clusters
     */
    protected void writeClusters(Context context, Collection<ICluster> clusters) throws IOException, InterruptedException {
        for (ICluster cluster : clusters) {
            writeCluster(context, cluster);
        }
    }

    /**
     * Write a single cluster
     */
    protected void writeCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        writeVettedCluster(context, cluster);
    }

    /**
     * Write a single vetted cluster
     */
    protected void writeVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        IKeyable key = makeClusterOutputKey(cluster);

        String clusterText = IOUtilities.convertClusterToCGFString(cluster);

        keyOutputText.set(key.toString());
        valueOutputText.set(clusterText);

        // todo: remove this condition check in the future
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
}