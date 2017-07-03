package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Output clusters into .clustering file format using hadoop build in
 * output mechanism
 *
 * @author Rui Wang
 * @version $Id$
 */
public class ClusteringFileWriteReducer extends Reducer<Text, Text, NullWritable, Text> {

    private final Set<String> currentClusteredSpectraIds = new HashSet<>();

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // parse cluster
            String content = value.toString();
            ICluster cluster = IOUtilities.parseClusterFromCGFString(content);

            String combinedSpectraId = cluster.getSpectralId();
            if (!currentClusteredSpectraIds.contains(combinedSpectraId)) {
                // record the combined spectra id
                currentClusteredSpectraIds.add(combinedSpectraId);
                String clusterString = IOUtilities.convertClusterToClusteringString(cluster);
                outputValue.set(clusterString);
                context.write(NullWritable.get(), outputValue);
            }
        }
    }
}
