package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.ac.ebi.pride.spectracluster.hadoop.bin.APrioriBinning;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;

/**
 * A partitioner which guarantees that given a key representing a BinMZKey that
 * all value with a given bin go to the same reducer
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class BinPartitioner extends Partitioner<Text, Text> {
    private APrioriBinning prePartitioning;
    private final IWideBinner binner = ClusterHadoopDefaults.DEFAULT_WIDE_MZ_BINNER;
    private final String binningResource = ClusterHadoopDefaults.getBinningResource();

    @Override
    public int getPartition(Text key, Text value, int numberOfReducers) {
        String keyStr = key.toString();

        BinMZKey binMZKey = new BinMZKey(keyStr);

        if (getPrePartitioning(numberOfReducers) != null) {
            int bin = getPrePartitioning(numberOfReducers).getBin(binMZKey.getPrecursorMZ());
            if (bin != -1) {
                return bin;
            }
        }

        // if cannot find the bin, then using partition hash to choose the reducer
        int partitionHash = binMZKey.getPartitionHash();
        return partitionHash % numberOfReducers;
    }

    private APrioriBinning getPrePartitioning(int numberReducers) {
        if (prePartitioning == null || prePartitioning.getNumberOfBins() != numberReducers) {
            try {
                APrioriBinning prioriBinning = new APrioriBinning(binningResource, numberReducers, binner);
                setPrePartitioning(prioriBinning);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        return prePartitioning;
    }

    private void setPrePartitioning(APrioriBinning prePartitioning) {
        this.prePartitioning = prePartitioning;
    }
}
