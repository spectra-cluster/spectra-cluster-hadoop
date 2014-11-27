package uk.ac.ebi.pride.spectracluster.hadoop.bin;

import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Utility method for MarkedNumber
 *
 * todo: may be merge this class with APrioriBinning?
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public final class MarkedNumberUtilities {

    private MarkedNumberUtilities(){}

    /**
     * return a list with values adjusted to sum to 1
     */
    public static Map<Integer, Integer> partitionFromBinner(@Nonnull List<MarkedNumber<String>> inItems, int numberOfReducers, IWideBinner binner) {
        List<MarkedNumber<String>> items = normalize(inItems);
        double minBin = Double.MAX_VALUE;
        Map<String, MarkedNumber> toTotal = new HashMap<String, MarkedNumber>();
        for (MarkedNumber<String> item : items) {
            double value = item.getValue();
            if (value > 0)
                minBin = Math.min(minBin, value);
            toTotal.put(item.getMark(), item);
        }


        double[] totals = new double[numberOfReducers];
        Map<Integer, Integer> ret = new HashMap<Integer, Integer>();
        for (int i = binner.getMinBin(); i < binner.getMaxBin(); i++) {
            double mz = binner.fromBin(i);
            String s = describeDaltons(mz);
            int thisBin = findBestBin(totals);
            MarkedNumber thisMark = toTotal.get(s);
            if (thisMark == null)
                thisMark = new MarkedNumber(new Integer(i), minBin); // give not found the minimum weight
            ret.put(i, thisBin);
            totals[thisBin] += thisMark.getValue(); // put in this partition
        }

        return ret; // map from index to partition
    }

    public static String describeDaltons(double precursorMZ) {
        return "MZ" + String.format("%05d", (int) (precursorMZ + 0.5));
    }

    /**
     * return a list with values adjusted to sum to 1
     */
    public static <T> Map<T, Integer> partitionIntoBins(@Nonnull List<MarkedNumber<T>> inItems, int numberBins) {
        List<MarkedNumber<T>> items = normalize(inItems);
        Map<T, Integer> ret = new HashMap<T, Integer>();
        double[] totals = new double[numberBins];
        for (MarkedNumber<T> item : items) {
            int thisBin = findBestBin(totals);
            ret.put(item.getMark(), thisBin);
            totals[thisBin] += item.getValue(); // put in this partition
        }
        return ret; // map from index to partition
    }


    private static int findBestBin(double[] bins) {
        double bestValue = Double.MAX_VALUE;

        int bestIndex = -1;
        for (int i = 0; i < bins.length; i++) {
            double bin = bins[i];
            if (bin == 0)
                return i; // always use empty bin
            if (bin < bestValue) {
                bestIndex = i;
                bestValue = bin; // now we have the smallest
            }
        }

        return bestIndex;
    }

    /**
     * return a list with values adjusted to sum to 1
     *
     * @param items some list
     * @param <T>   type
     * @return
     */
    public static <T> List<MarkedNumber<T>> normalize(@Nonnull List<MarkedNumber<T>> items) {
        double total = total(items);
        if (total == 1)
            return items;

        List<MarkedNumber<T>> holder = new ArrayList<MarkedNumber<T>>();
        for (MarkedNumber<T> item : items) {
            holder.add(new MarkedNumber<T>(item.getMark(), item.getValue() / total));
        }

        Collections.sort(holder);

        return holder;
    }

    private static <T> double total(@Nonnull List<MarkedNumber<T>> items) {
        double ret = 0;
        for (MarkedNumber<T> item : items) {
            ret += item.getValue();
        }
        return ret;
    }
}
