package uk.ac.ebi.pride.spectracluster.hadoop.spectrum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopClusterProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.normalizer.IIntensityNormalizer;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.function.peak.FractionTICPeakFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemoveImpossiblyHighPeaksFunction;
import uk.ac.ebi.pride.spectracluster.util.function.spectrum.RemovePrecursorPeaksFunction;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * SpectraToClusterMapper converts a spectrum into a cluster, and also performs the following steps:
 *
 * 1. Filter out a spectrum when its precursor m/z value is above a defined m/z threshold
 * 2. Normalise the filtered spectrum
 * 3. Convert the spectrum into a cluster
 * 4. Generate an unique id for the cluster
 *
 * @author Rui Wang
 * @version $Id$
 */
public class SpectrumToClusterMapper extends Mapper<Writable, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    /**
     * Reuse normalizer
     */
    private IIntensityNormalizer intensityNormalizer = Defaults.getDefaultIntensityNormalizer();
    //private IFunction<ISpectrum, ISpectrum> initialSpectrumFilter = Functions.join(new RemoveImpossiblyHighPeaksFunction(), new RemovePrecursorPeaksFunction(Defaults.getFragmentIonTolerance()));
    private IFunction<ISpectrum, ISpectrum> initialSpectrumFilter =  Functions.join(
            new RemoveImpossiblyHighPeaksFunction(),
            new RemovePrecursorPeaksFunction(Defaults.getFragmentIonTolerance()));
    private IFunction<List<IPeak>, List<IPeak>> peakFilter = new FractionTICPeakFunction(0.5F, 25);

    private static final double BIN_OVERLAP = 0;
    private static final float DEFAULT_BIN_WIDTH = 2F;
    private static final boolean OVERFLOW_BINS = true;
    private static final double LOWEST_MZ = 0;

    private double binWidth;
    private IWideBinner binner;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());

        binWidth = context.getConfiguration().getFloat(SpectrumToClusterJob.WINDOW_SIZE_PROPERTY, DEFAULT_BIN_WIDTH);

        binner = new SizedWideBinner(
                MZIntensityUtilities.HIGHEST_USABLE_MZ, binWidth, LOWEST_MZ, BIN_OVERLAP, OVERFLOW_BINS);
    }

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // check the validity of the input
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        // read the original content as MGF
        ISpectrum spectrum = IOUtilities.parseSpectrumFromMGFString(value.toString());

        float precursorMz = spectrum.getPrecursorMz();

        if (precursorMz < MZIntensityUtilities.HIGHEST_USABLE_MZ) {
            // increment dalton bin counter
            //CounterUtilities.incrementDaltonCounters(precursorMz, context);

            // remove impossible peaks and limit to 150
            ISpectrum filteredSpectrum = initialSpectrumFilter.apply(spectrum);

            ISpectrum normalisedSpectrum = normaliseSpectrum(filteredSpectrum);

            // only retain the signal peaks
            ISpectrum reducedSpectrum = new Spectrum(filteredSpectrum, peakFilter.apply(normalisedSpectrum.getPeaks()));

            // generate a new cluster
            ICluster cluster = ClusterUtilities.asCluster(reducedSpectrum);

            // get the bin(s)
            int[] bins = binner.asBins(cluster.getPrecursorMz());
            // this is the expected behaviour as there are no overlaps defined
            if (bins.length == 1) {
                cluster.setProperty(HadoopClusterProperties.SPECTRUM_TO_CLUSTER_BIN, String.valueOf(bins[0]));
                // increment the counter
                // - number of counters is limited to 120...
                //context.getCounter("precursor_bins", String.format("%s%d", HadoopClusterProperties.BIN_PREFIX, bins[0])).increment(1);
            }
            else {
                throw new InterruptedException("This implementation only works if now overlap is set during binning.");
            }

            // generate an unique id for the cluster
            cluster.setId(UUID.randomUUID().toString());

            // output cluster
            MZKey mzKey = new MZKey(precursorMz);
            keyOutputText.set(mzKey.toString());
            valueOutputText.set(IOUtilities.convertClusterToCGFString(cluster));
            context.write(keyOutputText, valueOutputText);
        }
    }


    /**
     * Normalise all the peaks within a spectrum
     *
     * @param originalSpectrum original input spectrum
     * @return normalised spectrum
     */
    private ISpectrum normaliseSpectrum(ISpectrum originalSpectrum) {
        List<IPeak> normalizedPeaks = intensityNormalizer.normalizePeaks(originalSpectrum.getPeaks());
        return new Spectrum(originalSpectrum, normalizedPeaks);
    }
}
