package uk.ac.ebi.pride.spectracluster.keys;


import uk.ac.ebi.pride.spectracluster.hadoop.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ChargePeakMZKey
 * key represents charge peak and precursor mz
 * User: Steve
 * Date: 8/13/13
 * key object using charge,mz,peak
 */
public class PeakMZKey implements  Comparable<PeakMZKey>,IKeyable{

    private final double peakMZ;
    private final double precursorMZ;
    private String asString;
    private int partitionHash;

    @SuppressWarnings("UnusedDeclaration")
    public PeakMZKey(final double pPeakMZ, final double pPrecursorMZ) {
        peakMZ = pPeakMZ;
        precursorMZ = pPrecursorMZ;
        asString = null;
    }

    @SuppressWarnings("UnusedDeclaration")
    public PeakMZKey(String str) {
        final String[] split = str.split(":");
        String key1 = split[0];
        peakMZ = SpectraHadoopUtilities.keyToMZ(key1);
        String key2 = split[1];
        precursorMZ = SpectraHadoopUtilities.keyToMZ(key2);
        asString = null;    // force string regeneration
    }

    /**
     * MZ_RESOLUTION * peakMZ
     * @return
     */
    public double getPeakMZ() {
        return peakMZ;
    }

    /**
      * MZ_RESOLUTION * getPrecursorMZ
      * @return
      */
    public double getPrecursorMZ() {
        return precursorMZ;
    }

    @Override
    public String toString() {
        if (asString == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(SpectraHadoopUtilities.mzToKey(getPeakMZ()));
            sb.append(":");
            partitionHash = sb.toString().hashCode();
            sb.append(SpectraHadoopUtilities.mzToKey(getPrecursorMZ()));

            asString = sb.toString();
        }
        return asString;

    }

    @Override
    public boolean equals(final Object o) {
         //noinspection SimplifiableIfStatement
        if(getClass() != o.getClass())
             return false;
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    /**
     * sort by string works
     * @param o
     * @return
     */
    @Override
    public int compareTo(final PeakMZKey o) {
        return toString().compareTo(o.toString());
    }

    /**
     * here is an int that a partitioner would use
     * @return
     */
    public int getPartitionHash() {
        //noinspection UnusedDeclaration
        int x = hashCode(); // force toString to be called once.
        return Math.abs(partitionHash);
    }
}
