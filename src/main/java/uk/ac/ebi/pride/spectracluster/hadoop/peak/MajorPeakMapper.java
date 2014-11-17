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
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakMapper extends Mapper<Writable, Text, Text, Text> {

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // check the validity of the input
        String label = key.toString();
        String originalContent = value.toString();

        if (label.length() == 0 || originalContent.length() == 0)
            return;

        // read the original content as MGF
        ISpectrum spectrum = parseSpectrumFromString(originalContent);

        // todo: do we need to check whether spectrum is null ?

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
                context.write(new Text(keyStr), new Text(spectrumString));
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
        IIntensityNormalizer intensityNormalizer = Defaults.getDefaultIntensityNormalizer();
        List<IPeak> normalizedPeaks = intensityNormalizer.normalizePeaks(originalSpectrum.getPeaks());
        return new Spectrum(originalSpectrum, normalizedPeaks);
    }
}
