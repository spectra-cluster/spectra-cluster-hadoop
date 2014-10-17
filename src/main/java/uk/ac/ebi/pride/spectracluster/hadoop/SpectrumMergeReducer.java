package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.engine.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;

import javax.annotation.*;
import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SpectrumMergeReducer.
 * Form clusters from peaks
 */
public class SpectrumMergeReducer extends AbstractClusteringEngineReducer {

    private double spectrumMergeWindowSize = HadoopDefaults.getSpectrumMergeMZWindowSize();  // this was not initialized!! 7/22/14 SL

    private final Set<String> writtenSpectralIDSThisBin = new HashSet<String>();
    private final Set<String> seenSpectrumSpectralIDSThisBin = new HashSet<String>();

    @SuppressWarnings("UnusedDeclaration")
    public double getMajorMZ() {
        return getMajorPeak();
    }


    public double getSpectrumMergeWindowSize() {
        return spectrumMergeWindowSize;
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        boolean offsetBins = context.getConfiguration().getBoolean("offsetBins", false);
        if (offsetBins)
            setBinner((IWideBinner) getBinner().offSetHalf());

    }

    @Override
    public void reduceNormal(Text key, Iterable<Text> values,
                             Context context) throws IOException, InterruptedException {

        String keyStr = key.toString();
        //    System.err.println(keyStr);
        BinMZKey mzKey = new BinMZKey(keyStr);
        if (mzKey.getBin() < 0) {
            System.err.println("Bad bin " + keyStr);
            return;
        }

        // we only need to change engines for different charges
        if (mzKey.getBin() != getCurrentBin() ||
                getEngine() == null) {
            boolean usedata = updateEngine(context, mzKey);
            if (!usedata)
                return;
        }

        IIncrementalClusteringEngine engine = getEngine();

        int numberProcessed = 0;
        int numberNoremove = 0;
        int numberRemove = 0;

        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();

            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);

            if (cluster != null) {  // todo why might this happen
                if (engine != null) {     // todo why might this happen
                    // look in great detail at a few cases
//                    if (isInterestingCluster(cluster)) {
//                        Collection<ICluster> clusters = engine.getClusters();
//                        ClusterSimilarityUtilities.testAddToClusters(cluster, clusters); // break here
//                    }

                    // remember spectrum ids seen added 22/7
                    // todo SL hope this does not cause memory issues
                    List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
                    if (clusteredSpectra.size() == 1) {
                        if (seenSpectrumSpectralIDSThisBin.contains(clusteredSpectra.get(0).getId()))
                            continue; // ignore single clusters where we have seen the spectrum
                    }
                    for (ISpectrum spc : clusteredSpectra) {
                        seenSpectrumSpectralIDSThisBin.add(spc.getId());
                    }


                    final Collection<ICluster> removedClusters = engine.addClusterIncremental(cluster);
                    if (!removedClusters.isEmpty()) {
                        writeClusters(context, removedClusters);
                        numberRemove++;
                    } else
                        numberNoremove++;

                }
            }
            if (numberProcessed > 0 && numberProcessed % 100 == 0)
                getBinTime().showElapsed("processed " + numberProcessed, System.err);
            //     System.err.println("processed " + numberProcessed);
            numberProcessed++;
        }
    }

//    private static String[] DUPLICATE_IDS = {
//            "VYYFQGGNNELGTAVGK", //   606.51,606.54
//            "YEEQTTNHPVAIVGAR",   //    595.7100228.2
//            "WAGNANELNAAYAADGYAR",  // 666.67797
//            "AKQPVKDGPLSTNVEAK"
//
//    };

//    private static Set<String> INTERESTING_IDS = new HashSet<String>(Arrays.asList(DUPLICATE_IDS));


//    protected static boolean isInterestingCluster(ICluster test) {
//        List<ISpectrum> clusteredSpectra = test.getClusteredSpectra();
//        for (ISpectrum spc : clusteredSpectra) {
//            if (spc instanceof ISpectrum) {
//                String peptide = ((ISpectrum) spc).getPeptide();
//                if (peptide != null && INTERESTING_IDS.contains(peptide))
//                    return true;
//            }
//        }
//        return false;
//    }

    /**
     * @param context
     * @param cluster
     * @return true if we still want to write
     */
    protected boolean trackDuplicates(@Nonnull final Context context, @Nonnull final ICluster cluster) {

        /**
         * this entire section is here to track duplicates and stop writing single spectra when
         * they are already clustered
         */
        int clusteredSpectraCount = cluster.getClusteredSpectraCount();
        if (clusteredSpectraCount == 0)
            return false; // empty don't bother
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        if (clusteredSpectraCount == 1) {
            ISpectrum onlySpectrum = clusteredSpectra.get(0);
            if (writtenSpectralIDSThisBin.contains(onlySpectrum.getId())) {
                Counter counter = context.getCounter("Duplicates", "attempt_single");
                counter.increment(1);
                return false; // already written
            }
        } else {
            for (ISpectrum spc : clusteredSpectra) {
                Counter counter = context.getCounter("Duplicates", "add_spectrum");
                counter.increment(1);

                String id = spc.getId();
                if (!writtenSpectralIDSThisBin.contains(id)) {
                    writtenSpectralIDSThisBin.add(id); // track when written
                } else {
                    counter = context.getCounter("Duplicates", "add_duplicate");
                    counter.increment(1);
                }
            }
        }
        return true; // go process
    }

    /**
     * this version of writeCluster does all the real work
     *
     * @param context
     * @param cluster
     * @throws IOException
     * @throws InterruptedException
     */
    protected void writeOneVettedCluster(@Nonnull final Context context, @Nonnull final ICluster cluster) throws IOException, InterruptedException {
        /**
         * is a duplicate  so ignore   added SL 22/7
         */
        if (!trackDuplicates(context, cluster))
            return;

//        if (isInterestingCluster(cluster))
//            System.out.println(cluster.toString());

        IWideBinner binner1 = getBinner();
        float precursorMz = cluster.getPrecursorMz();
        int bin = binner1.asBin(precursorMz);
        // you can merge clusters outside the current bin but not write them
        if (bin != getCurrentBin()) {
            // track when this happens
            String offString = bin > getCurrentBin() ? "above" : "below";
            Counter counter = context.getCounter("OutsideBin", offString);
            counter.increment(1);
            return;
        }
        MZKey key = new MZKey(precursorMz);

        StringBuilder sb = new StringBuilder();
        final CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        String string = sb.toString();

        if (string.length() > SpectraHadoopUtilities.MIMIMUM_CLUSTER_LENGTH) {
            writeKeyValue(key.toString(), string, context);

        }
    }

    /**
     * switching to a new engine add code for resting instrumentation
     *
     * @param pEngine
     */
    @Override
    public void setEngine(final IIncrementalClusteringEngine pEngine) {
        super.setEngine(pEngine);
        writtenSpectralIDSThisBin.clear(); // reset ids
        seenSpectrumSpectralIDSThisBin.clear(); // reset  seen ids
    }

    /**
     * make a new engine because  either we are in a new peak or at the end (pMZKey == null
     *
     * @param context !null context
     */
    protected <T> boolean updateEngine(final Context context, final T key) throws IOException, InterruptedException {
        BinMZKey pMzKey = (BinMZKey) key;
        if (getEngine() != null) {
            final Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
            setEngine(null);
        }
        boolean ret = true;
        // if not at end make a new engine
        if (pMzKey != null) {
            setEngine(getFactory().getIncrementalClusteringEngine((float) getSpectrumMergeWindowSize()));
            setMajorPeak(pMzKey.getPrecursorMZ());
            try {
                ret = setCurrentBin(pMzKey.getBin());
            } catch (IllegalArgumentException e) {
                return false; // todo do better SLewis very large bins - say 8250 can be ignores
            }
        }
        return ret;
    }


    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        //    writeParseParameters(context);
        super.cleanup(context);
        int value;

        value = IncrementalClusteringEngine.numberOverlap;
        System.err.println("numberOverlap " + value);

        value = IncrementalClusteringEngine.numberNotMerge;
        System.err.println("numberNotMerge " + value);

        value = IncrementalClusteringEngine.numberLessGoodMerge;
        System.err.println("numberLessGoodMerge " + value);

        value = IncrementalClusteringEngine.numberGoodMerge;
        System.err.println("numberGoodMerge " + value);

        value = IncrementalClusteringEngine.numberGoodMerge;
        System.err.println("numberGoodMerge " + value);

        value = IncrementalClusteringEngine.numberReAsssigned;
        System.err.println("numberReAsssigned " + value);

    }

    /**
     * remember we built the database
     *
     * @param context
     * @throws java.io.IOException
     */
    @SuppressWarnings({"UnusedParameters", "UnusedDeclaration"})
    protected void writeParseParameters(final Context context) throws IOException {
        throw new UnsupportedOperationException("Unimplemented June 2 This"); // ToDo
//        Configuration cfg = context.getConfiguration();
//        HadoopTandemMain application = getApplication();
//        TaskAttemptID tid = context.getTaskAttemptID();
//        //noinspection UnusedDeclaration
//        String taskStr = tid.getTaskID().toString();
//        String paramsFile = application.getDatabaseName() + ".params";
//        Path dd = HadoopUtilities.getRelativePath(paramsFile);
//
//        FileSystem fs = FileSystem.get(cfg);
//
//        if (!fs.exists(dd)) {
//            try {
//                FastaHadoopLoader ldr = new FastaHadoopLoader(application);
//                String x = ldr.asXMLString();
//                FSDataOutputStream fsout = fs.create(dd);
//                PrintWriter out = new PrintWriter(fsout);
//                out.println(x);
//                out.close();
//            }
//            catch (IOException e) {
//                try {
//                    fs.delete(dd, false);
//                }
//                catch (IOException e1) {
//                    throw new RuntimeException(e1);
//                }
//                throw new RuntimeException(e);
//            }
//
//        }

    }


}
