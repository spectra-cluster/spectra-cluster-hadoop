package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.MarkedNumber;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.cluster.APrioriBinningTests
 * User: Steve
 * Date: 4/24/2014
 */
public class APrioriBinningTests {

    public static final int NUMBER_BINS = 300;

    @Test
    public void testDefaultBinning() {
        APrioriBinning<String> binning = new APrioriBinning(NUMBER_BINS, HadoopDefaults.DEFAULT_WIDE_MZ_BINNER);
        List<MarkedNumber<String>> markedNumbers = APrioriBinning.readFromResource();
        markedNumbers = MarkedNumber.normalize(markedNumbers);
        double[] values = new double[NUMBER_BINS];
        int[] bins = new int[markedNumbers.size()];
        int index = 0;
        for (MarkedNumber<String> markedNumber : markedNumbers) {
            String current = markedNumber.getMark();
            current = current.substring(2); // drop MZ
            double daltons = MZIntensityUtilities.asDaltons(current);
            int bin = binning.getBin(daltons);
            bins[index++] = bin;
            double value = markedNumber.getValue();
            values[bin] += value;
        }
        for (int i = 0; i < bins.length; i++) {
            int bin = bins[i];
            //          System.out.println(" " + bin + " " + values[bin]);
        }
    }

}
