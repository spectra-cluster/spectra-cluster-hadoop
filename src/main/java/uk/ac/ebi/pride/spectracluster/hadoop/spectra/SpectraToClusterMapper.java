package uk.ac.ebi.pride.spectracluster.hadoop.spectra;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.normalizer.IIntensityNormalizer;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

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
public class SpectraToClusterMapper extends Mapper<Writable, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    /**
     * Reuse normalizer
     */
    private IIntensityNormalizer intensityNormalizer = Defaults.getDefaultIntensityNormalizer();

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
            CounterUtilities.incrementDaltonCounters(precursorMz, context);

            // normalise all spectrum
            ISpectrum normaliseSpectrum = normaliseSpectrum(spectrum);

            // generate a new cluster
            ICluster cluster = ClusterUtilities.asCluster(normaliseSpectrum);

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
