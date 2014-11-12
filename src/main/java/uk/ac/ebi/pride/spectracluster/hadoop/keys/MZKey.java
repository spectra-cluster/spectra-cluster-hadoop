package uk.ac.ebi.pride.spectracluster.hadoop.keys;


import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

import java.io.Serializable;

/**
 * key object using  mz
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 *
 * <p/>
 */
public class MZKey implements Comparable<MZKey>, Serializable, IPartitionable {

    private final double precursorMZ;
    private final String precursorMZKey;

    public MZKey(final double pPrecursorMZ) {
        precursorMZ = pPrecursorMZ;
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }

    public MZKey(String str) {
        precursorMZ = KeyUtilities.keyToMZ(str);
        precursorMZKey = KeyUtilities.mzToKey(getPrecursorMZ());
    }


    public double getPrecursorMZ() {
        return precursorMZ;
    }

    public int getAsInt() {
        return (int) precursorMZ;
    }

    @Override
    public String toString() {
        return precursorMZKey;
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
     * here is an int that a partitioner would use
     *
     * @return
     */
    public int getPartitionHash() {
        return (int) (getPrecursorMZ() * MZIntensityUtilities.MZ_RESOLUTION + 0.5);
    }


    /**
     * sort by string works
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(final MZKey o) {
        return toString().compareTo(o.toString());
    }
}
