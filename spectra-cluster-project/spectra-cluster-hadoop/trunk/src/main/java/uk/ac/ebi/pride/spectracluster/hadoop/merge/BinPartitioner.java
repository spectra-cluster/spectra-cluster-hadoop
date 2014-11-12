package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopDefaults;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

/**
 * A partitioner which guarantees that given a key representing a BinMZKey that
 * all value with a given bin go to the same reducer
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class BinPartitioner extends Partitioner<Text, Text> {
    private static APrioriBinning<StableBinMZKey> prePartitioning;
    private final static IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;

    @Override
    public int getPartition(Text key, Text value, int numberOfReducers) {
        String keyStr = key.toString();

        BinMZKey binMZKey = new BinMZKey(keyStr);



        return 0;
    }

    public static APrioriBinning<StableBinMZKey> getPrePartitioning(int numberReducers) {
        if (prePartitioning == null || prePartitioning.getNumberBins() == numberReducers)
            setPrePartitioning(new APrioriBinning(numberReducers, binner));
        return prePartitioning;
    }

    public static void setPrePartitioning(APrioriBinning<StableBinMZKey> prePartitioning) {
        BinPartitioner.prePartitioning = prePartitioning;
    }
}
