package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.ac.ebi.pride.spectracluster.keys.PeakMZKey;

/**
 * A partitioner which guarantees that given a key representing a PeakMZ that
 * all value with a given peak go to the same reducer
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text text, Text text2, int numberOfReducers) {
        PeakMZKey realKey = new PeakMZKey(text.toString());

        // the partition hash uses only the first two elements charge and peak
        int hash = realKey.getPartitionHash();

        return hash % numberOfReducers;
    }

}
