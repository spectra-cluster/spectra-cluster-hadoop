package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import uk.ac.ebi.pride.spectracluster.keys.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.AbstractBinnedAPrioriPartitioner
 * User: Steve
 * Date: 4/24/2014
 */
public abstract class AbstractBinnedAPrioriPartitioner extends Partitioner<Text, Text> {
    protected static APrioriBinning<StableBinMZKey> prepartitioning;
    private static IWideBinner binner;

    public static APrioriBinning<StableBinMZKey> getPrepartitioning(int numberReducers) {
        if (prepartitioning == null || prepartitioning.getNumberBins() == numberReducers)
            setPrepartitioning(new APrioriBinning(numberReducers,getBinner()));
        return prepartitioning;
    }

    public static void setPrepartitioning(final APrioriBinning<StableBinMZKey> pPrepartitioning) {
        prepartitioning = pPrepartitioning;
    }

    public static IWideBinner getBinner() {
        return binner;
    }

    public static void setBinner(final IWideBinner pBinner) {
        binner = pBinner;
    }
}
