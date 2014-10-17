package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.*;
import com.lordjoe.utilities.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.clustersmilarity.*;
import uk.ac.ebi.pride.spectracluster.engine.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.similarity.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SampleRunner
 * User: Steve
 * Date: 7/23/2014
 */
public class SampleRunner {


    private double spectrumMergeWindowSize = HadoopDefaults.getSpectrumMergeMZWindowSize();

    private final Map<String, List<ICluster>> keysToClusters = new HashMap<String, List<ICluster>>();
    private final List<ICluster> inputClusters = new ArrayList<ICluster>();
    private final List<ICluster> foundClusters = new ArrayList<ICluster>();
    private final CountedMap<String> inputCounts = new CountedMap<String>();
    private final CountedMap<String> outputCounts = new CountedMap<String>();
    private final Set<String> highDuplicates = new HashSet<String>();
    private final Set<String> seenHighDuplicates = new HashSet<String>();

    protected int currentCharge;
    private int currentBin;

    private IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;
    private IncrementalClusteringEngineFactory factory = new IncrementalClusteringEngineFactory();


    public SampleRunner(List<ICluster> clusters) {
        inputClusters.addAll(clusters);
        performMapping(clusters);
        List<String> itemsMoreThanN = inputCounts.getItemsMoreThanN(2);
        highDuplicates.addAll(itemsMoreThanN);
    }

    /**
     * true if the cluster has a highly duplucated spectrum - we want to look at these cases
     * SL
     *
     * @param cluster
     * @return
     */
    public ISpectrum getHighReplication(ICluster cluster) {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        for (ISpectrum spc : clusteredSpectra) {
            String id = spc.getId();
            if (highDuplicates.contains(id)) {
                if (seenHighDuplicates.contains(id)) {
           //         System.out.println(id);
                    return spc;
                }
                else {
                    seenHighDuplicates.add(id);
                }
            }
        }
        return null;
    }

    public int totalMapped() {
        int ret = 0;
        for (String s : keysToClusters.keySet()) {
            ret += keysToClusters.get(s).size();
        }
        return ret;
    }

    public int getCurrentCharge() {
        return currentCharge;
    }

    public void setCurrentCharge(final int pCurrentCharge) {
        currentCharge = pCurrentCharge;
    }

    public int getCurrentBin() {
        return currentBin;
    }

    public void setCurrentBin(final int pCurrentBin) {
        currentBin = pCurrentBin;
    }

    public double getSpectrumMergeWindowSize() {
        return spectrumMergeWindowSize;
    }

    public void setSpectrumMergeWindowSize(final double pSpectrumMergeWindowSize) {
        spectrumMergeWindowSize = pSpectrumMergeWindowSize;
    }

    public void analyze() {
        String[] keys = getSortedKeys();
        System.out.println("Number bins " + keys.length);


        BinMZKey key = new BinMZKey(keys[0]);
        setCurrentBin(key.getBin());
        int currentPartition = key.getPartitionHash();
        IIncrementalClusteringEngine engine = factory.getIncrementalClusteringEngine((float) getSpectrumMergeWindowSize());
        System.out.println("new Engine " + key);
        for (int i = 0; i < keys.length; i++) {
            String s = keys[i];
            key = new BinMZKey(s);
            if (key.getPartitionHash() != currentPartition) {
                for (ICluster iCluster : engine.getClusters()) {
                    saveCluster(key, iCluster);
                }
                engine = factory.getIncrementalClusteringEngine((float) getSpectrumMergeWindowSize());

                currentPartition = key.getPartitionHash();
                System.out.println("new Engine " + key);
            }
            reduceBin(key, engine);
        }
        for (ICluster iCluster : engine.getClusters()) {
            saveCluster(key, iCluster);
        }
    }

    protected void reduceBin(BinMZKey key, IIncrementalClusteringEngine engine) {
        List<ICluster> clusters = keysToClusters.get(key.toString());
        reduceBin(engine, key, clusters);
    }

    protected void reduceBin(IIncrementalClusteringEngine engine, BinMZKey key, List<ICluster> clusters) {
        int highReplication = 0;
        for (ICluster cluster : clusters) {
            ISpectrum duplicateSpc = getHighReplication(cluster);
            if (duplicateSpc != null)
                examineHighReplication(duplicateSpc, cluster, engine);
            Collection<ICluster> toSaveClusters = engine.addClusterIncremental(cluster);
            for (ICluster toSaveCluster : toSaveClusters) {
//                duplicateSpc = getHighReplication(toSaveCluster);
//                if (duplicateSpc != null)
//                    System.out.println("Write High Duplicate " + duplicateSpc.getId());
                saveCluster(key, toSaveCluster);
            }
        }
    }

    /**
     * walk tests on clusters with overlap
     *
     * @param pDuplicateSpc
     * @param pCluster
     * @param engine
     */
    protected boolean examineHighReplication(final ISpectrum pDuplicateSpc, final ICluster pCluster, final IIncrementalClusteringEngine engine) {
        List<ICluster> holder = new ArrayList<ICluster>();
        for (ICluster clstr : engine.getClusters()) {
            Set<String> spectralIds = clstr.getSpectralIds();
            if (spectralIds.contains(pCluster.getId()))
                holder.add(clstr);
        }
        ISimilarityChecker cker = engine.getSimilarityChecker();
        ISpectrum consensusSpectrum = pCluster.getConsensusSpectrum();
        Set<String> spectralIds = pCluster.getSpectralIds();
        double highestSimilarityScore = 0;
        ICluster bestmatch = null;
        for (ICluster testCluster : holder) {
            ISpectrum consensusSpectrum1 = testCluster.getConsensusSpectrum();

            double similarityScore = cker.assessSimilarity(consensusSpectrum, consensusSpectrum1);
            if (similarityScore > highestSimilarityScore) {
                highestSimilarityScore = similarityScore;
                bestmatch = testCluster;
            }
            Set<String> spectraOverlap = IncrementalClusteringEngine.getSpectraOverlap(spectralIds, testCluster);
            Set<String> spectraOverlap2 = ClusterSimilarityUtilities.getSpectraOverlap(spectralIds, testCluster);
            double score = ClusterUtilities.clusterFullyContainsScore(pCluster, testCluster);
        }
        boolean ret = handlePotentialOverlap(pCluster, bestmatch, highestSimilarityScore);
        return ret;
    }


    /**
     * if there are overlapping spectra among the current cluster and the best match
     * then  firure out what is best
     *
     * @param cluster1
     * @param cluster2
     * @return
     */
    //  @Deprecated // TODO JG Reevaluate the usage of this function   Emulates removed function SL
    protected boolean handlePotentialOverlap(final ICluster cluster1, final ICluster cluster2, double highestSimilarityScore) {
        if (highestSimilarityScore < IncrementalClusteringEngine.MINIMUM_SIMILARITY_SCORE_FOR_OVERLAP)
            return false;     // we did nothing
        Set<String> ids = cluster1.getSpectralIds();
        Set<String> best = cluster2.getSpectralIds();
        Set<String> spectraOverlap = IncrementalClusteringEngine.getSpectraOverlap(ids, cluster2);
        int numberOverlap = spectraOverlap.size();
        if (numberOverlap == 0)
            return false; // no overlap
        int minClusterSize = Math.min(best.size(), ids.size());

        // of a lot of overlap then force a merge
        if (numberOverlap >= minClusterSize / 2) {  // enough overlap then merge
            //       mergeIntoCluster(cluster1, cluster2);
            return true;
        }
        // allow a bonus for overlap
        double similarityThreshold = Defaults.getSimilarityThreshold();
        if (highestSimilarityScore + IncrementalClusteringEngine.BONUS_PER_OVERLAP > similarityThreshold) {
            //        mergeIntoCluster(cluster1, cluster2);
            return true;

        }


        // force overlappping spectra into the best cluster
        //      return assignOverlapsToBestCluster(cluster1, cluster2, spectraOverlap);

        return false;
    }


    protected void saveCluster(BinMZKey key, final ICluster pToSaveCluster) {
        int bin = binner.asBin(pToSaveCluster.getPrecursorMz());
        if (key.getBin() != bin)
            return;
        foundClusters.add(pToSaveCluster);
        addCluster(pToSaveCluster, outputCounts);
    }

    protected String[] getSortedKeys() {
        List<String> ret = new ArrayList<String>(keysToClusters.keySet());
        Collections.sort(ret);
        return ret.toArray(new String[ret.size()]);
    }

    public static void addCluster(ICluster cluster, CountedMap<String> counts) {
        for (ISpectrum spc : cluster.getClusteredSpectra()) {
            counts.add(spc.getId());
        }
    }


    public void reportCounts(final Appendable pOut) {


        System.out.println("Input Clusters " + inputClusters.size());
        System.out.println("Mapped Clusters " + totalMapped());
        System.out.println("Output Clusters " + foundClusters.size());


        System.out.println("Input counts " + inputCounts.getTotal());
        double[] countDistribution = inputCounts.getCountDistribution();
        for (int i = 1; i < countDistribution.length; i++) {
            System.out.println(i + " " + String.format("%8.5f", countDistribution[i]));

        }
        System.out.println("Output counts " + outputCounts.getTotal());
        countDistribution = outputCounts.getCountDistribution();
        for (int i = 1; i < countDistribution.length; i++) {
            System.out.println(i + " " + String.format("%8.5f", countDistribution[i]));

        }
    }

    public List<ICluster> getFoundClusters() {
        return foundClusters;
    }

    protected void performMapping(List<ICluster> clusters) {
        /**
         * what uk.ac.ebi.pride.spectracluster.hadoop.ChargeMZNarrowBinMapper
         * does
         *
         */
        for (ICluster cluster : clusters) {
            addCluster(cluster, inputCounts);
        //    int precursorCharge = cluster.getPrecursorCharge();
            double precursorMZ = cluster.getPrecursorMz();
            int[] bins = binner.asBins(precursorMZ);
            //noinspection ForLoopReplaceableByForEach
            for (int j = 0; j < bins.length; j++) {
                int bin = bins[j];
                BinMZKey mzKey = new BinMZKey(bin, precursorMZ);
                String keyStr = mzKey.toString();
                if (keysToClusters.containsKey(keyStr)) {
                    keysToClusters.get(keyStr).add(cluster);
                }
                else {
                    List<ICluster> list = new ArrayList<ICluster>();
                    list.add(cluster);
                    keysToClusters.put(keyStr, list);
                }
            }

        }

    }

    /**
     * little piece of code to sample every nth cluster in a cgf file
     *
     * @param args
     * @param every
     * @throws IOException
     */
    public static void writeClusters(ICluster[] args, int every) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter("Saved" + every + ".cgf"));
        for (int i = 0; i < args.length; i += 3 * every) {
            // try sequences
            ICluster arg = args[i];
            CGFClusterAppender.INSTANCE.appendCluster(pw, arg);
            arg = args[i + 1];
            CGFClusterAppender.INSTANCE.appendCluster(pw, arg);
            arg = args[i + 2];
            CGFClusterAppender.INSTANCE.appendCluster(pw, arg);
        }
        pw.close();
    }


    /**
     * little piece of code to sample every the first nth
     *
     * @param args
     * @param every
     * @throws IOException
     */
    public static void writeFirstClusters(ICluster[] args, int every) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter("First" + every + "th.cgf"));
        for (int i = 0; i < args.length / every; i++) {
            // try sequences
            ICluster arg = args[i];
            CGFClusterAppender.INSTANCE.appendCluster(pw, arg);
        }
        pw.close();
    }


    /**
     * remerge clusters - this should cause things to get better
     *
     * @param runner
     * @return
     */
    public static SampleRunner reanalyze(SampleRunner runner) {
        SampleRunner runner2 = new SampleRunner(runner.foundClusters);
        runner2.analyze();
        runner2.reportCounts(System.out);
        return runner2;
    }

    public static void main(String[] args) throws Exception {
        ElapsedTimer timer = new ElapsedTimer();
        final String fileName = args[0];
        ICluster[] clusters = ParserUtilities.readSpectralCluster(new File(fileName));
        // Save a small sample for development
        // writeClusters(clusters,8);
        // writeFirstClusters(clusters, 5);

        final List<ICluster> initialClusters = Arrays.asList(clusters);
        List<IClusterSet> clusterSets = new ArrayList<IClusterSet>();

        SimpleClusterSet sc = new SimpleClusterSet(initialClusters);
        sc.setName(fileName);
        clusterSets.add(sc);

        SampleRunner runner = new SampleRunner(initialClusters);
        timer.showElapsed("read cgf");
        timer.reset();
        runner.analyze();
        timer.showElapsed("analyzed");
        runner.reportCounts(System.out);

        for (int i = 0; i < 3; i++) {
            System.out.println("===========================");
            timer.reset();

            sc = new SimpleClusterSet(runner.foundClusters);
            sc.setName(fileName);
            clusterSets.add(sc);

            runner = reanalyze(runner);
            timer.showElapsed("analyzed");
        }


        System.out.println("Done");
    }

}
