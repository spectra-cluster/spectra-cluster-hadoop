package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.io.MGFSpectrumAppender;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.normalizer.IIntensityNormalizer;
import uk.ac.ebi.pride.spectracluster.spectrum.IPeak;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.Spectrum;
import uk.ac.ebi.pride.spectracluster.util.Defaults;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.List;

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
        ISpectrum spectrum = parseSpectrumFromString(value.toString());

        float precursorMz = spectrum.getPrecursorMz();

        if (precursorMz < MZIntensityUtilities.HIGHEST_USABLE_MZ) {
            // increment dalton bin counter
            CounterUtilities.incrementDaltonCounters(precursorMz, context);

            // normalise all spectrum
            ISpectrum normaliseSpectrum = normaliseSpectrum(spectrum);

            String spectrumString = convertSpectrumToString(normaliseSpectrum);

            for (int peakMz : normaliseSpectrum.asMajorPeakMZs(Defaults.getMajorPeakCount())) {
                PeakMZKey mzKey = new PeakMZKey(peakMz, precursorMz);

                final String keyStr = mzKey.toString();
                keyOutputText.set(keyStr);
                valueOutputText.set(spectrumString);
                context.write(keyOutputText, valueOutputText);
            }
        }
    }

    /**
     * Convert spectrum to string
     *
     * @param spectrum given spectrum
     * @return string represents spectrum
     */
    private String convertSpectrumToString(ISpectrum spectrum) {
        StringBuilder sb = new StringBuilder();
        MGFSpectrumAppender.INSTANCE.appendSpectrum(sb, spectrum);
        return sb.toString();
    }

    /**
     * Parse a given string into a spectrum
     *
     * @param originalContent given string content
     * @return parsed spectrum
     */
    private ISpectrum parseSpectrumFromString(String originalContent) {
        LineNumberReader reader = new LineNumberReader(new StringReader(originalContent));

        try {
            return ParserUtilities.readMGFScan(reader);
        } catch (Exception e) {
            throw new IllegalStateException("Error while parsing spectrum", e);
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
