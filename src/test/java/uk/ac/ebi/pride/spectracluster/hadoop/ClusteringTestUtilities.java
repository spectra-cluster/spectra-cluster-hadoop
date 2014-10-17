package uk.ac.ebi.pride.spectracluster.hadoop;


import org.junit.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.util.ClusteringTestUtilities
 * User: Steve
 * Date: 6/19/13
 */
public class ClusteringTestUtilities {

    public static final String SAMPLE_MGF_FILE = "uk/ac/ebi/pride/spectracluster/hadoop/spectra_400.0_4.0.mgf";

    public static final String SAMPLE_CGF_FILE = "uk/ac/ebi/pride/spectracluster/hadoop/spectra_400.0_4.0.cgf";


    /**
     * final an assertion of all clusters om the set are not equivalent
     *
     * @param pScs
     * @param pScs2
     */
    public static void assertEquivalentClusters(final List<ICluster> pScs, final List<ICluster> pScs2) {
        final ClusterContentComparator comparator = ClusterContentComparator.INSTANCE;
        Collections.sort(pScs, comparator);
        Collections.sort(pScs2, comparator);
        Assert.assertEquals(pScs.size(), pScs2.size());
        for (int i = 0; i < pScs.size(); i++) {
            ICluster cl1 = pScs.get(i);
            ICluster cl2 = pScs2.get(i);
            boolean equivalent = cl1.equivalent(cl2);
            Assert.assertTrue(equivalent);

        }
    }

    /**
     * read a resource mgf as a list of spectra
     *
     * @return
     */
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
        File inputFile = getSpectrumFile(resName);

        ISpectrum[] mgfSpectra = ParserUtilities.readMGFScans(inputFile);
        return Arrays.asList(mgfSpectra);
    }

    /**
     * read a resource mgf as a list of spectra
     *
     * @return
     */
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
        File inputFile = getSpectrumFile(resName);


        ISpectrum[] mgfSpectra = ParserUtilities.readMGFScans(inputFile);
        return Arrays.asList(mgfSpectra);
    }


    public static File getSpectrumFile(String resName) {
        // load a file contains a list of clusters
        URL url;
        url = ClusteringTestUtilities.class.getClassLoader().getResource(resName);
        if (url == null) {
            throw new IllegalStateException("no file for input found!");
        }
        File inputFile;
        try {
            inputFile = new File(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);

        }
        return inputFile;
    }

    public static List<ICluster> readSpectraClustersFromResource() {
        return readSpectraClustersFromResource(SAMPLE_CGF_FILE);
    }

    public static List<ICluster> readSpectraClustersFromResource(String resName) {
        List<ConsensusSpectraItems> items = readConsensusSpectraItemsFromResource(resName);
        int index = 1000;
        List<ICluster> holder = new ArrayList<ICluster>();
        for (ConsensusSpectraItems si : items) {
            ICluster cluster = new SpectralCluster(Integer.toString(index++), Defaults.getDefaultConsensusSpectrumBuilder());
            for (ISpectrum sr : si.getSpectra())
                cluster.addSpectra(sr);
            holder.add(cluster);
        }

        return holder;
    }

    /**
     * read a resource mgf as a list of spectra
     *
     * @param resName
     * @return
     */
    public static List<ConsensusSpectraItems> readConsensusSpectraItemsFromResource(String resName) {
        try {
            // load a file contains a list of clusters
            URL url = ClusteringTestUtilities.class.getClassLoader().getResource(resName);
            if (url == null) {
                throw new IllegalStateException("no file for input found!");
            }
            File inputFile = new File(url.toURI());

            //noinspection UnusedDeclaration,UnnecessaryLocalVariable
            List<ConsensusSpectraItems> consensusSpectraItems = Arrays.asList(ParserUtilities.readClusters(inputFile));
            return consensusSpectraItems;

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);

        }

    }


}


