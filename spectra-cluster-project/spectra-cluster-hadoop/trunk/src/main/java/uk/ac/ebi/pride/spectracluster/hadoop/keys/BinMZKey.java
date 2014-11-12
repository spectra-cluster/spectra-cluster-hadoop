package uk.ac.ebi.pride.spectracluster.hadoop.keys;


import java.io.Serializable;

/**
 * Key represents bin number and precursor mz
 *
 * @author Steve Lewis
 * @author Rui Wang
 */
public class BinMZKey implements Comparable<BinMZKey>, IPartitionable, Serializable {

    private final int bin;
    private final double precursorMZ;
    private final String binKey;
    private final String precursorMZKey;

    public BinMZKey(final int pBin, final double pPrecursorMZ) {
        bin = pBin;
        precursorMZ = pPrecursorMZ;
        binKey = String.format("%06d", getBin());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public BinMZKey(String str) {
        String[] split = str.split(":");
        bin = Integer.parseInt(split[0]);
        precursorMZ = KeyUtilities.keyToMZ(split[1]);
        binKey = String.format("%06d", getBin());
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    /**
     * MZ_RESOLUTION * peakMZ
     *
     * @return
     */
    public int getBin() {
        return bin;
    }

    /**
     * MZ_RESOLUTION * getPrecursorMZ
     *
     * @return
     */
    public double getPrecursorMZ() {
        return precursorMZ;
    }

    @Override
    public String toString() {
        return binKey + ":" + precursorMZKey;
    }


    @Override
    public boolean equals(final Object o) {
        return o != null && getClass() == o.getClass() && toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    /**
     * sort by string works
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(final BinMZKey o) {
        return toString().compareTo(o.toString());
    }

    /**
     * here is an int that a partitioner would use
     *
     * @return
     */
    public int getPartitionHash() {
        return Math.abs(binKey.hashCode());
    }
}
