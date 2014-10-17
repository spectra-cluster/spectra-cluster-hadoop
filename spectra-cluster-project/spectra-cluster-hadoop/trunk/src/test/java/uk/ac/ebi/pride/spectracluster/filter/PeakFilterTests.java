package uk.ac.ebi.pride.spectracluster.filter;

import org.junit.*;
import uk.ac.ebi.pride.spectracluster.hadoop.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;

import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.filter.PeakFilterTests
 *
 * @author Steve Lewis
 * @date 28/05/2014
 */
public class PeakFilterTests {

    /**
      * validate encoding for a number of spectra
      *
      * @throws Exception
      */
     @Test
     public void testPeaks() throws Exception {
         IPeakFilter filter = new BinnedHighestNPeakFilter() ;
         List<ISpectrum> originalSpectra = ClusteringTestUtilities.readISpectraFromResource();
         for (ISpectrum spc : originalSpectra) {
             validateFilteredPeaks(spc,filter);
         }

     }


    private void validateFilteredPeaks(ISpectrum spc,IPeakFilter filter) {
        final List<IPeak> peaks = spc.getPeaks();
        Set<IPeak> original = new HashSet<IPeak>(peaks);
        final List<IPeak> filteredPeaks = filter.filter(peaks);
        ISpectrum newSpectrum = new Spectrum(spc,filteredPeaks);

    }


}
