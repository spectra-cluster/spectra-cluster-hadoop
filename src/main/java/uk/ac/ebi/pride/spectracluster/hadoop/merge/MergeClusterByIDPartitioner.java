package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MergeClusterByIDPartitioner partitions the compute space using the key has which contains the ID
 *
 * @author Rui Wang
 * @version $Id$
 */
@Deprecated
public class MergeClusterByIDPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numberOfReducers) {
        String keyStr = key.toString();
        return keyStr.hashCode() % numberOfReducers;
    }
}

