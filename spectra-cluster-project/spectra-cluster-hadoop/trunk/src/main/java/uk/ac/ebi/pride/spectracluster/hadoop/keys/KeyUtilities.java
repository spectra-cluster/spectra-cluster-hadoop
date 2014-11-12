package uk.ac.ebi.pride.spectracluster.hadoop.keys;

import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class KeyUtilities {

    /**
     * convert am int into an mz for easy comparison
     *
     * @param mz input
     * @return MZ_RESOLUTION * mz as int
     */
    public static String mzToKey(double mz) {
        int peak = MZIntensityUtilities.mzToInt(mz);
        return String.format("%010d", peak);
    }

    /**
     * convert an int into an mz for east comparison
     *
     * @param key input
     * @return MZ_RESOLUTION * mz as int
     */
    public static double keyToMZ(String key) {
        double ret = Integer.parseInt(key); // (double)MZ_RESOLUTION;
        return ret / MZIntensityUtilities.MZ_RESOLUTION;
    }
}
