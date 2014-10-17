package uk.ac.ebi.pride.spectracluster.keys;


import uk.ac.ebi.pride.spectracluster.hadoop.*;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ChargeMZKey
 * User: Steve
 * Date: 8/13/13
 * key object using  mz
 */
public class MZKey implements Comparable<MZKey>,IKeyable {

    private final double precursorMZ;
    private final int asInt;
    private String asString;

    @SuppressWarnings("UnusedDeclaration")
    public MZKey(final double pPrecursorMZ) {
        precursorMZ = pPrecursorMZ;
        asInt = (int)precursorMZ;
         asString = null;    // force string regeneration
    }

    @SuppressWarnings("UnusedDeclaration")
    public MZKey(String str) {
        precursorMZ = SpectraHadoopUtilities.keyToMZ(str);
        asInt = (int)precursorMZ;
        asString = null;    // force string regeneration
    }


    public double getPrecursorMZ() {
        return precursorMZ;
    }

    public int getAsInt() {
        return asInt;
    }

    @Override
    public String toString() {
        if (asString == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(SpectraHadoopUtilities.mzToKey(getPrecursorMZ()));

            asString = sb.toString();
        }
        return asString;

    }

    @Override
    public boolean equals(final Object o) {
        if (o == null)
            return false;
        //noinspection SimplifiableIfStatement
        if (getClass() != o.getClass())
            return false;
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * here is an int that a partitioner would use
     * @return
     */
    @SuppressWarnings("UnusedDeclaration")
    public int getPartitionHash() {
        int ret = (int)(getPrecursorMZ() * MZIntensityUtilities.MZ_RESOLUTION + 0.5);
        return ret;
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
