package uk.ac.ebi.pride.spectracluster.hadoop;


import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.consensus.*;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.similarity.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;


/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ClusteringDataUtilities
 * User: Steve
 * Date: 8/13/13
 */
public class ClusteringDataUtilities {

    public static final String SAMPLE_MGF_FILE = "spectra_400.0_4.0.mgf";
    @SuppressWarnings("UnusedDeclaration")
    public static final String SAMPLE_CGF_FILE = "spectra_400.0_4.0.cgf";

    /**
     * read a resource mgf as a list of spectra
     *
     * @return !null list
     */
    @SuppressWarnings("UnusedDeclaration")
    public static List<ISpectrum> readConsensusSpectralItems() {
        return readConsensusSpectralItems(SAMPLE_MGF_FILE);
    }

    /**
     * read a resource mgf as a list of spectra
     *
     * @param resName
     * @return
     */
    public static List<ISpectrum> readConsensusSpectralItems(String resName) {
        // load a file contains a list of clusters
        final InputStream resourceAsStream;
        final ClassLoader classLoader;
        classLoader = ClusteringDataUtilities.class.getClassLoader();
        resourceAsStream = classLoader.getResourceAsStream(resName);
        if (resourceAsStream == null) {
            throw new IllegalStateException("no file for input found!");
        }
        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(resourceAsStream));

        ISpectrum[] mgfSpectra = ParserUtilities.readMGFScans(rdr);
        return Arrays.asList(mgfSpectra);
    }


    /**
     * read a resource mgf as a list of spectra
     *
     * @return !null list
     */
    @SuppressWarnings("UnusedDeclaration")
    public static List<ISpectrum> readISpectraFromResource() {
        return readISpectraFromResource(SAMPLE_MGF_FILE);
    }

    /**
     * read a resource mgf as a list of spectra
     *
     * @param resName
     * @return
     */
    public static List<ISpectrum> readISpectraFromResource(String resName) {
        // load a file contains a list of clusters
        final InputStream resourceAsStream;
        final ClassLoader classLoader;
        classLoader = ClusteringDataUtilities.class.getClassLoader();
        resourceAsStream = classLoader.getResourceAsStream(resName);
        if (resourceAsStream == null) {
            throw new IllegalStateException("no input found!");
        }
        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(resourceAsStream));
        ISpectrum[] mgfSpectra = ParserUtilities.readMGFScans(rdr);
        return Arrays.asList(mgfSpectra);
    }


    @SuppressWarnings("UnusedDeclaration")
    public static final int MAX_PEAK_MISMATCHES = 3;
    public static final int NUMBER_HIGH_MATCHES = 6;
    public static final double FORGIVEN_MZ_DIFFERENCE = 0.2;
    public static final double FORGIVEN_INTENSITY_RATIO = 0.1;
    public static final double FORGIVEN_INTENSITY_DIFFERENCE = 200;

    /**
     * tolerate a few errors
     *
     * @param sp1
     * @param sp2
     * @return
     */
    @SuppressWarnings("UnusedDeclaration")
    public static boolean areSpectraVeryClose(ISpectrum sp1, ISpectrum sp2) {

        // easy case
        if (sp1.equivalent(sp2))
            return true;

        // Find mismatched peaks
        //noinspection MismatchedQueryAndUpdateOfCollection
        List<IPeak> mismatched = new ArrayList<IPeak>();
        double diff = Math.abs(sp1.getPrecursorMz() - sp2.getPrecursorMz());
        if (diff > MZIntensityUtilities.SMALL_MZ_DIFFERENCE)
            return false;

        if (sp1.getPeaksCount() != sp1.getPeaksCount())
            return false;

        int numberMisMatchedPeaks = 0;
        List<IPeak> l1 = sp1.getPeaks();
        List<IPeak> l2 = sp2.getPeaks();
        for (int i = 0; i < l1.size(); i++) {
            IPeak p1 = l1.get(i);
            IPeak p2 = l2.get(i);
            if (!p1.equivalent(p2)) {
                mismatched.add(p1);
                mismatched.add(p2);
                numberMisMatchedPeaks++;
            }
        }
        if (numberMisMatchedPeaks == 0)
            return true;  // probably will nopt happen

        final ISimilarityChecker checker = Defaults.getDefaultSimilarityChecker();


        // well we better agree on the highest peaks
        final ISpectrum hp1 = sp1.getHighestNPeaks(NUMBER_HIGH_MATCHES);
        final ISpectrum hp2 = sp2.getHighestNPeaks(NUMBER_HIGH_MATCHES);
        l1 = hp1.getPeaks();
        l2 = hp2.getPeaks();
        for (int i = 0; i < l1.size(); i++) {
            IPeak p1 = l1.get(i);
            IPeak p2 = l2.get(i);
            double diffx = Math.abs(p1.getMz() - p2.getMz());
            // allow larger MZ diffrences
            if (diffx > FORGIVEN_MZ_DIFFERENCE)
                return false;

            double avgIntensity1 = p1.getIntensity() / p1.getCount();
            double avgIntensity2 = p2.getIntensity() / p2.getCount();
            double totalIntensity = avgIntensity1 + avgIntensity2;
            double diffI = Math.abs((avgIntensity1 / totalIntensity) - (avgIntensity2 / totalIntensity));
            // allow larger MZ diffrences
            if (diffI > FORGIVEN_INTENSITY_RATIO) {
                if (Math.abs(avgIntensity1 - avgIntensity2) > FORGIVEN_INTENSITY_DIFFERENCE)
                    return false;
            }

        }
        // ignore count and intensity diferences
        // dot product
        double similarity = checker.assessSimilarity(sp1, sp2);
        // certainly in the same cluster
        double defaultThreshold = Defaults.getSimilarityThreshold();  // 0.6 - 0.7
        //noinspection UnusedDeclaration
        double highThreshold = 1.0 - ((1.0 - defaultThreshold) / 2); // 0.8 - 0.85
        //noinspection RedundantIfStatement
        if (similarity < defaultThreshold)     // similarity better be 0.95
            return false;  // dot producta not close enough

        return true;

    }


    @SuppressWarnings("UnusedDeclaration")
    public static boolean peakListsEquivalent(List<IPeak> l1, List<IPeak> l2) {
        if (l1.size() != l2.size())
            return false;
        for (int i = 0; i < l1.size(); i++) {
            IPeak p1 = l1.get(i);
            IPeak p2 = l2.get(i);
            if (!p1.equivalent(p2))
                return false;
        }
        return true;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static boolean areNewPeakListsEquivalent(List<IPeak> l1, List<IPeak> l2, boolean print) {
        boolean isEqual = true;

        for (int i = 0; i < l1.size(); i++) {
            IPeak p1 = l1.get(i);
            IPeak p2 = l2.get(i);

            if (print)
                System.out.format(i + ": new = old\tm/z: %f = %f\t\tintens: %f = %f\tcount: %d = %d", p1.getMz(), p2.getMz(), p1.getIntensity(), p2.getIntensity(), p1.getCount(), p2.getCount());

            if (p1.getCount() != p2.getCount()) {
                if (print) System.out.println(" <-- count differs!");
                isEqual = false;
            }

            else if (p1.getMz() != p2.getMz()) {
                if (print) System.out.println(" <-- m/z differ!");
                isEqual = false;
            }
            else if (p1.getIntensity() != p2.getIntensity()) {
                if (print) System.out.println(" <-- intensity differ!");
                isEqual = false;
            }
            else {
                if (print) System.out.println("");
            }
        }
        return isEqual;
    }

    /**
     * create a list of consensusSpectra from a list of clusters
     *
     * @param pClusters !null cluster list
     * @return !null list of  consensusSpectra
     */
    @SuppressWarnings("UnusedDeclaration")
    public static List<ISpectrum> buildConsensusSpectra(final List<ICluster> pClusters, final IConsensusSpectrumBuilder consensusSpectrumBuilder) {
        List<ISpectrum> holder = new ArrayList<ISpectrum>();
        for (ICluster cluster : pClusters) {

            final List<ISpectrum> css = cluster.getClusteredSpectra();
            for (ISpectrum cs : css) {
                consensusSpectrumBuilder.addSpectra(cs);
            }

            final ISpectrum oldSpec = consensusSpectrumBuilder.getConsensusSpectrum();
            holder.add(oldSpec);
        }


        return holder;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static List<ICluster> readSpectraClustersFromResource() {
        return readSpectraClustersFromResource(SAMPLE_CGF_FILE);
    }


    public static List<ICluster> readSpectraClustersFromResource(String resName) {
        // load a file contains a list of clusters
        final InputStream resourceAsStream;
        final ClassLoader classLoader;
        classLoader = ClusteringDataUtilities.class.getClassLoader();
        resourceAsStream = classLoader.getResourceAsStream(resName);
        if (resourceAsStream == null) {
            throw new IllegalStateException("no file for input found!");
        }
        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(resourceAsStream));

        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);
        //noinspection UnnecessaryLocalVariable
        List<ICluster> holder = new ArrayList<ICluster>(Arrays.asList(clusters));

        return holder;
    }


}



