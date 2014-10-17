package uk.ac.ebi.pride.spectracluster.keys;


import uk.ac.ebi.pride.spectracluster.hadoop.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ChargePeakMZKey
 * key represents charge bin number and precursor mz
 * this change forces a commit
 * User: Steve
 * Date: 8/13/13
 * key object using charge,mz,peak
 */
public class BinMZKey implements Comparable<BinMZKey>,IKeyable {

    private final int bin;
    private final double precursorMZ;
    private String asString;
    private int partitionHash;

      public BinMZKey(final int pBin, final double pPrecursorMZ) {
        bin = pBin;
        precursorMZ = pPrecursorMZ;
        asString = null;
    }

     public BinMZKey(String str) {
        final String[] split = str.split(":");
        String key1 = split[0];
        bin = Integer.parseInt(key1);
        String key2 = split[1];
        precursorMZ = SpectraHadoopUtilities.keyToMZ(key2);
        asString = null;    // force string regeneration
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
        if (asString == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%06d", getBin()));
            // only include charge and bin
            partitionHash = sb.toString().hashCode();
            sb.append(":");
            double precursorMZ1 = getPrecursorMZ();
            sb.append(SpectraHadoopUtilities.mzToKey(precursorMZ1));

            asString = sb.toString();
        }
        return asString;

    }


    @Override
    public boolean equals(final Object o) {
        if(o == null)
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
        //noinspection UnusedDeclaration
        int x = hashCode(); // force toString to be called once.
        return Math.abs(partitionHash);
    }
}
