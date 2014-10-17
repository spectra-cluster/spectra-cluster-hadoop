package uk.ac.ebi.pride.spectracluster.hadoop;

import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.hadoop.datastore.SpectrumUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.util.ArrayList;
import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.spectrum.SpectrumUtilitiesTests
 * User: Steve
 * Date: 7/16/13
 */
public class SpectrumUtilitiesTests {

    /**
     * validate encoding for a number of spectra
     *
     * @throws Exception
     */
    @Test
    public void testPeaks() throws Exception {
        List<ISpectrum> originalSpectra = ClusteringTestUtilities.readISpectraFromResource();
        for (ISpectrum spc : originalSpectra) {
            validateEncode(spc);
        }

    }

    /**
     * make sure the orginal peaks are the same as the endoded and decoded peaks
     *
     * @param spc !null spactrum
     */
    protected void validateEncode(ISpectrum spc) {
        List<IPeak> peaks = spc.getPeaks();
        if (peaks.size() > SpectrumUtilities.MAXIMUM_ENCODED_PEAKS) {
            List<IPeak> copy = new ArrayList<IPeak>(peaks);
            final List<IPeak> newpeaks = SpectrumUtilities.filterTop250Peaks(copy);
            copy.removeAll(newpeaks);
            Assert.assertEquals(SpectrumUtilities.MAXIMUM_ENCODED_PEAKS, newpeaks.size());
            Assert.assertEquals(peaks.size() - SpectrumUtilities.MAXIMUM_ENCODED_PEAKS, copy.size());
            peaks = newpeaks;
        }
        final String s = SpectrumUtilities.peaksToDataString(peaks);
        final List<IPeak> newpeaks = SpectrumUtilities.dataStringToPeaks(s);

        Assert.assertEquals(peaks.size(), newpeaks.size());

        for (int i = 0; i < peaks.size(); i++) {
            IPeak p1 = peaks.get(i);
            IPeak p2 = newpeaks.get(i);
            Assert.assertTrue(p1.equivalent(p2));

        }

    }
}
