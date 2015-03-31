package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;

import java.io.IOException;

/**
 * MergeClusterByIDMapper reads the clusters and send them off using cluster id as the key
 *
 * @author Rui Wang
 * @version $Id$
 */
public class MergeClusterByIDMapper extends Mapper<Text, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        ICluster cluster = IOUtilities.parseClusterFromCGFString(value.toString());

        keyOutputText.set(cluster.getId());
        valueOutputText.set(value.toString());
        context.write(keyOutputText, valueOutputText);
    }
}
