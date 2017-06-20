package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

/**
 *
 * Counter related utility methods
 *
 * @author Steve Lewis
 * @author Rui Wang
 *
 */
public class CounterUtilities {

    /**
     * Increment the counter for a particular bin
     * <p/>
     * NOTE: bin - a particular m/z range
     * <p/>
     *
     * @param context     Hadoop context
     * @param mz clustering m/z
     */
    public static void incrementMajorPeakCounters(Mapper.Context context, double mz) {
        String counterName = String.format("%05d", mz).trim();
        Counter counter = context.getCounter("MajorPeak", counterName);
        counter.increment(1);
    }

    /**
     * Increment the counter for a particular bin
     * <p/>
     * NOTE: bin - a particular m/z range
     * <p/>
     *
     * @param precursorMZ precursor m/z
     * @param context     Hadoop context
     */
    public static void incrementDaltonCounters(float precursorMZ, Mapper.Context context) {
        Counter counter = context.getCounter("Binning", MZIntensityUtilities.describeDaltons(precursorMZ));
        counter.increment(1);
    }


    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param hash    retucer assuming  HadoopUtilities.DEFAULT_NUMBER_REDUCERS is right
     */
    public static void incrementPartitionCounter(Mapper.Context context, String prefix, int hash) {
        String counterName = prefix + String.format("%05d", hash).trim();
        context.getCounter("Partition", counterName).increment(1);
    }


    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param hash    retucer assuming  HadoopUtilities.DEFAULT_NUMBER_REDUCERS is right
     */
    public static void incrementPartitionCounter(Reducer.Context context, String prefix, int hash) {
        String counterName = prefix + String.format("%05d", hash).trim();
        context.getCounter("Partition", counterName).increment(1);
    }

    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param mzKey   !null key
     */
    public static void incrementPartitionCounter(Mapper.Context context, PeakMZKey mzKey) {
        int hash = mzKey.getPartitionHash() % context.getNumReduceTasks();
        incrementPartitionCounter(context, "Peak", hash);
    }

    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param mzKey   !null key
     */
    public static void incrementPartitionCounter(Mapper.Context context, BinMZKey mzKey) {
        int hash = mzKey.getPartitionHash() % context.getNumReduceTasks();
        incrementPartitionCounter(context, "Bin", hash);
    }
}