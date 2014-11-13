package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;

/**
 * A partitioner which guarantees that given a key representing a MZKey that
 * all value with a given charge and bin go to the same reducer
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numberOfReducers) {
        String label = key.toString();

        MZKey mzKey = new MZKey(label);

        int hash = mzKey.getAsInt();
        return hash % numberOfReducers;
    }
}
