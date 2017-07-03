package uk.ac.ebi.pride.spectracluster.hadoop.bin;

import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

import java.io.IOException;
import java.util.List;

public class APrioriBinningTest {

    private static final int NUMBER_BINS = 300;

    @Test
    public void testDefaultBinning() throws IOException {
        APrioriBinning binning = new APrioriBinning(ClusterHadoopDefaults.DEFAULT_BINNING_RESOURCE, NUMBER_BINS, ClusterHadoopDefaults.DEFAULT_WIDE_MZ_BINNER);
        List<MarkedNumber<String>> markedNumbers = BinningReader.read(ClusterHadoopDefaults.DEFAULT_BINNING_RESOURCE);
        markedNumbers = MarkedNumberUtilities.normalize(markedNumbers);
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

        for (int bin : bins) {
//            System.out.println(" " + bin + " " + values[bin]);
        }
    }

}