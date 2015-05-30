package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.merge.MZNarrowBinMapper;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

import java.io.IOException;

/**
 * Mapper that gets the highest peaks of a spectrum and then send corresponding copies of spectra
 * along with the highest peak to the reducer
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakMapper extends Mapper<Writable, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    private static final double BIN_WIDTH = 1.0;
    private static final double BIN_OVERLAP = 0;
    private static final boolean OVERFLOW_BINS = true;
    private static final IWideBinner binner = new SizedWideBinner
            (MZIntensityUtilities.HIGHEST_USABLE_MZ, BIN_WIDTH, MZIntensityUtilities.LOWEST_USABLE_MZ, BIN_OVERLAP, OVERFLOW_BINS);

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // check the validity of the input
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        // read the original content as cluster
        ICluster cluster = IOUtilities.parseClusterFromCGFString(value.toString());

        // precursor m/z
        float precursorMz = cluster.getPrecursorMz();

        // increment dalton bin counter
        CounterUtilities.incrementDaltonCounters(precursorMz, context);

        // bin according the precursor mz
        int[] bins = binner.asBins(precursorMz);

        for (int bin : bins) {
            BinMZKey binMZKey = new BinMZKey(bin, precursorMz);
            keyOutputText.set(binMZKey.toString());
            valueOutputText.set(value.toString());
            context.write(keyOutputText, valueOutputText);
        }
    }
}
