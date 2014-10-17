package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.IWideBinner;
import com.lordjoe.algorithms.MarkedNumber;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * uk.ac.ebi.pride.spectracluster.cluster.APrioriBinning
 * used by Hadoop to bin based on known bin sizes
 * User: Steve
 * Date: 4/22/2014
 */
public class APrioriBinning<T> {

    public static final String DEFAULT_RESOURCE = "/PrideBinning.tsv";

    public static List<MarkedNumber<String>> readFromResource() {
        return readFromResource(DEFAULT_RESOURCE);
    }

    public static List<MarkedNumber<String>> readFromResource(String realName) {
        try {
            InputStream resourceAsStream = APrioriBinning.class.getResourceAsStream(DEFAULT_RESOURCE);
            LineNumberReader rdr = new LineNumberReader(new InputStreamReader(resourceAsStream));
            String line = rdr.readLine();
            line = rdr.readLine(); // drop header
            List<MarkedNumber<String>> holder = new ArrayList<MarkedNumber<String>>();

            while (line != null) {
                String[] items = line.split("\t");
                if (items.length == 2) {
                    MarkedNumber<String> mark = new MarkedNumber<String>(items[0], Double.parseDouble(items[1]));
                    holder.add(mark);
                }
                line = rdr.readLine();

            }
            return holder;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    private final Map occurrences;
    private final IWideBinner binner;
    private final int numberBins;

    public <T> APrioriBinning(List<MarkedNumber<String>> priors, int pnumberBins, IWideBinner pBinner) {
        binner = pBinner;
        numberBins = pnumberBins;
        occurrences = MarkedNumber.partitionFromBinner(priors, numberBins, pBinner);
    }

    public int getNumberBins() {
        return numberBins;
    }

    public <T> APrioriBinning(int numberBins, IWideBinner pBinner) {
        this(readFromResource(), numberBins, pBinner);
    }

    public int getBin(double mybin) {
        Integer binNumber = binner.asBin(mybin);
        if (occurrences.containsKey(binNumber))
            return (Integer) occurrences.get(binNumber);
        throw new IllegalArgumentException("no bin for " + mybin);
    }

    public double aprorioriProbability(double mz) {
        throw new UnsupportedOperationException("Unimplemented Fix This"); // ToDo
    }


    public double cummulativeAprorioriProbability(double mz) {
        throw new UnsupportedOperationException("Unimplemented Fix This"); // ToDo
    }

}
