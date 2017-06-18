package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.merge.MZNarrowBinMapper;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopClusterProperties;
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

    private static final double BIN_OVERLAP = 0;
    private static final float DEFAULT_BIN_WIDTH = 4F;
    private static final boolean OVERFLOW_BINS = true;
    private static final double LOWEST_MZ = 0;

    private double binWidth;
    private IWideBinner binner;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());

        binWidth = context.getConfiguration().getFloat(MajorPeakJob.CURRENT_BINNER_WINDOW_SIZE, DEFAULT_BIN_WIDTH);

        binner = new SizedWideBinner
                (MZIntensityUtilities.HIGHEST_USABLE_MZ, binWidth, LOWEST_MZ, BIN_OVERLAP, OVERFLOW_BINS);
    }

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // check the validity of the input
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        // read the original content as cluster
        ICluster cluster = IOUtilities.parseClusterFromCGFString(value.toString());

        // use the spectrum to cluster bin
        int bin = -1;

        // get the bin mapping
        String spectrumToClusterBin = cluster.getProperty(HadoopClusterProperties.SPECTRUM_TO_CLUSTER_BIN);

        if (spectrumToClusterBin != null) {
            int spectrumBin = new Integer(spectrumToClusterBin);
            // use the mapped bin
            bin = context.getConfiguration().getInt(String.format("mapping_%d", spectrumBin), -1);
            if (bin >= 0) {
                context.getCounter("Binning Procedure", "updated-bin").increment(1);
            }
        }

        // as a fall back bin use the original binner
        if (bin < 0) {
            // precursor m/z
            float precursorMz = cluster.getPrecursorMz();
            // bin according the precursor mz
            bin = binner.asBins(precursorMz)[0];
            context.getCounter("Binning Procedure", "re-mapped bin").increment(1);
        }

        BinMZKey binMZKey = new BinMZKey(bin, cluster.getPrecursorMz());
        keyOutputText.set(binMZKey.toString());
        valueOutputText.set(IOUtilities.convertClusterToCGFString(cluster));
        context.write(keyOutputText, valueOutputText);
    }
}
