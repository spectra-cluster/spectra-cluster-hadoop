package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.IWideBinner;
import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.KeyTests
 * User: Steve
 * Date: 8/13/13
 */
public class KeyTests {

    @Test
    public void testKeys() throws Exception {
        List<PeakMZKey> keyholder = new ArrayList<PeakMZKey>();
        final List<? extends ISpectrum> specs = ClusteringDataUtilities.readISpectraFromResource();
        for (ISpectrum spec : specs) {
            keyholder.addAll(validateSpectrum(spec));
        }
        validatePartitioner(keyholder)  ;
    }

    public static final int NUMBER_BUCKETS = 20;
    protected void validatePartitioner(List<PeakMZKey> keyholder) {
        int[] counts = new int[NUMBER_BUCKETS];
        for (PeakMZKey key : keyholder) {
             int hash = key.getPartitionHash();
             counts[hash % NUMBER_BUCKETS]++;
        }
        int maxCounts = 0;
        int minCounts = Integer.MAX_VALUE;
        //noinspection ForLoopReplaceableByForEach
       for (int i = 0; i < counts.length; i++) {
            int count = counts[i];
           maxCounts = Math.max(maxCounts,count);
           minCounts = Math.min(minCounts, count);

        }
        Assert.assertTrue( 1.3 * minCounts > maxCounts); // 30% of each other
    }

    protected List<PeakMZKey> validateSpectrum(ISpectrum spec) {
        final double precursorMz = spec.getPrecursorMz();
        final int charge = spec.getPrecursorCharge();

        MZKey mzKey1 = new MZKey(precursorMz);
        MZKey k2 = new MZKey(precursorMz);
        // better not crash

        String s1 = mzKey1.toString();
        String s2 = k2.toString();
        Assert.assertEquals(s2, String.format("%02d", charge) + ":" + s1);

        MZKey mzKey2 = new MZKey(s1);
        Assert.assertEquals(mzKey1,mzKey2);


        MZKey m3 = new MZKey(s2);
        Assert.assertEquals(k2,m3);

        PeakMZKey chargePeakKey1 = new PeakMZKey(precursorMz,precursorMz);
        PeakMZKey chargePeakKey2 = new PeakMZKey(chargePeakKey1.toString());
        if(!chargePeakKey1.equals(chargePeakKey2))
          Assert.assertEquals(chargePeakKey1,chargePeakKey2);

        IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;

        List<String> holder = new ArrayList<String>();
        List<PeakMZKey> keyholder = new ArrayList<PeakMZKey>();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        List<BinMZKey> binKeyholder = new ArrayList<BinMZKey>();
        for (IPeak pk : spec.getPeaks()) {
            final float mz = pk.getMz();
            PeakMZKey key = new PeakMZKey(mz, precursorMz);
            int[] bins = binner.asBins(precursorMz);
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < bins.length; i++) {
                int bin = bins[i];
                BinMZKey binkey = new BinMZKey(bin, precursorMz);
                binKeyholder.add(binkey) ;
                String binKeyStr = binkey.toString();
                BinMZKey binky2 = new BinMZKey(binKeyStr);
                 Assert.assertEquals(binkey,binky2);
            }
            String keyStr = key.toString();
            PeakMZKey ky2 = new PeakMZKey(keyStr);
            Assert.assertEquals(key,ky2);
            holder.add(keyStr);
            keyholder.add(key);
        }
        // make a sorted collection
        List<String> holderSort = new ArrayList<String>(holder);
        Collections.sort(holderSort);
        // because peaks ar MZ sorted keys should be
        Assert.assertArrayEquals(holderSort.toArray(),holder.toArray());

        // make a sorted collection
        List<PeakMZKey> keyholderSort = new ArrayList<PeakMZKey>(keyholder);
        Collections.sort(keyholderSort);
       // because peaks ar MZ sorted keys should be
        Assert.assertArrayEquals(keyholderSort.toArray(),keyholder.toArray());

        return keyholder;
    }
}
