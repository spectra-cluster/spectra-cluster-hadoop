package uk.ac.ebi.pride.spectracluster.hadoop.bin;

import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Used by Hadoop to bin based on known bin sizes
 *
 * todo: this class needs another review to understand the mechanism
 *
 * @author Steve Lewis
 * @author Rui Wang
 */
public class APrioriBinning {

    /**
     * First integer is the bin number in IWideBinner, second integer indicates which reducer to assign to
     */
    private final Map<Integer, Integer> occurrences;
    private final IWideBinner binner;
    private final int numberBins;

    public APrioriBinning(List<MarkedNumber<String>> priors, int pnumberBins, IWideBinner pBinner) {
        this.binner = pBinner;
        this.numberBins = pnumberBins;
        this.occurrences = MarkedNumberUtilities.partitionFromBinner(priors, numberBins, pBinner);
    }

    public APrioriBinning(String binningSource, int numberBins, IWideBinner pBinner) throws IOException {
        this.binner = pBinner;
        this.numberBins = numberBins;
        List<MarkedNumber<String>> markedNumbers = BinningReader.read(binningSource);
        this.occurrences = MarkedNumberUtilities.partitionFromBinner(markedNumbers, numberBins, pBinner);
    }

    public int getNumberOfBins() {
        return numberBins;
    }

    public int getBin(double mybin) {
        Integer binNumber = binner.asBin(mybin);
        if (occurrences.containsKey(binNumber))
            return occurrences.get(binNumber);

        return -1;
    }


}
