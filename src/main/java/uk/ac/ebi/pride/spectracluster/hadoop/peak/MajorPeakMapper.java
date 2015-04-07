package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

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

        // get consensus spectrum
        ISpectrum consensusSpectrum = cluster.getConsensusSpectrum();
        for (int peakMz : consensusSpectrum.asMajorPeakMZs(Defaults.getMajorPeakCount())) {
            // only forward none zero peaks
            if (peakMz > 0) {
                PeakMZKey mzKey = new PeakMZKey(peakMz, precursorMz);

                keyOutputText.set(mzKey.toString());
                valueOutputText.set(value.toString());
                context.write(keyOutputText, valueOutputText);
            }
        }
    }
}
