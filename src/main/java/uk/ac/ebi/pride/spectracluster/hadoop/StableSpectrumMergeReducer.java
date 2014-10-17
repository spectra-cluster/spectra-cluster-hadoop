package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.IWideBinner;
import org.apache.hadoop.io.Text;
import org.systemsbiology.hadoop.ISetableParameterHolder;
import uk.ac.ebi.pride.spectracluster.cluster.CountBasedClusterStabilityAssessor;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.cluster.IClusterStabilityAssessor;
import uk.ac.ebi.pride.spectracluster.engine.IStableClusteringEngine;
import uk.ac.ebi.pride.spectracluster.engine.StableClusteringEngine;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.keys.StableBinMZKey;
import uk.ac.ebi.pride.spectracluster.keys.UnStableBinMZKey;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;

/**
 * Merge spectra with unstable clusters
 */
public class StableSpectrumMergeReducer extends AbstractClusteringEngineReducer {

    private int currentGroup;
    private IClusterStabilityAssessor clusterStabilityAssessor = new CountBasedClusterStabilityAssessor();


    public int getCurrentGroup() {
        return currentGroup;
    }


    public void setCurrentGroup(int currentGroup) {
        this.currentGroup = currentGroup;
    }


    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        setBinner(StableClusterMapper.BINNER);
        ISetableParameterHolder application = getApplication();
        ConfigurableProperties.setStableClusterSizeFromProperties(application);
    }


    @Override
    public void reduceNormal(Text key, Iterable<Text> values,
                             Context context) throws IOException, InterruptedException {

        String keyStr = key.toString();
        //    System.err.println(keyStr);
        StableBinMZKey realKey;
        if (keyStr.contains(StableBinMZKey.SORT_PREFIX))
            realKey = new StableBinMZKey(keyStr);
        else {
            realKey = new UnStableBinMZKey(keyStr);
        }

        // we only need to change engines for different charges
        if (realKey.getBin() != getCurrentBin() ||
                realKey.getGroup() != getCurrentGroup() ||
                getEngine() == null) {
            updateEngine(context, realKey);
        }

        IStableClusteringEngine stableClusteringEngine = (IStableClusteringEngine) getEngine();

        int numberProcessed = 0;

        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();

            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);

            if (cluster != null && stableClusteringEngine != null) {  // todo why might this happen
                if (!clusterStabilityAssessor.isStable(cluster)) {
                    stableClusteringEngine.addUnstableCluster(cluster);
                } else {
                    stableClusteringEngine.processStableCluster(cluster);
                    List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
                    //
                    for (ISpectrum spc : clusteredSpectra) {
                        writeCluster(context, ClusterUtilities.asCluster(spc));
                    }
                }
            }
            if (numberProcessed % 100 == 0)
                getBinTime().showElapsed("processed " + numberProcessed, System.err);
            //     System.err.println("processed " + numberProcessed);
            numberProcessed++;
        }
    }

//
//    /**
//     * write one cluster and key
//     *
//     * @param context !null context
//     * @param cluster !null cluster
//     */
//    protected void writeCluster(final Context context, final ISpectralCluster cluster) throws IOException, InterruptedException {
//        final List<ISpectralCluster> allClusters = getClusteringEngine().findNoneFittingSpectra(cluster);
//        if (!allClusters.isEmpty()) {
//            for (ISpectralCluster removedCluster : allClusters) {
//
//                // drop all spectra
//                final List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
//                ISpectrum[] allRemoved = clusteredSpectra.toArray(new ISpectrum[clusteredSpectra.size()]);
//                cluster.removeSpectra(allRemoved);
//
//                // and write as stand alone
//                writeOneVettedCluster(context, removedCluster);
//            }
//
//        }
//        // now write the original
//        if (cluster.getClusteredSpectraCount() > 0)
//            writeOneVettedCluster(context, cluster);     // nothing removed
//    }

    protected void writeOneVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        if (cluster.getClusteredSpectraCount() == 0)
            return; // empty dont bother

        IWideBinner binner1 = getBinner();
        float precursorMz = cluster.getPrecursorMz();
        int bin = binner1.asBin(precursorMz);
        // you can merge clusters outside the current bin but not write them
        if (bin != getCurrentBin())
            return;
        MZKey key = new MZKey(precursorMz);

        StringBuilder sb = new StringBuilder();
        final CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        String string = sb.toString();

        if (string.length() > SpectraHadoopUtilities.MIMIMUM_CLUSTER_LENGTH) {
            writeKeyValue(key.toString(), string, context);

        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setMajorMZ(double majorMZ) {
        setMajorPeak(majorMZ);
    }


    /**
     * make a new engine because  either we are in a new peak or at the end (pMZKey == null
     *
     * @param context !null context
     */
    protected <T> boolean updateEngine(final Context context, T key) throws IOException, InterruptedException {
        final StableBinMZKey pMzKey = (StableBinMZKey) key;
        if (getEngine() != null) {
            Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
            setEngine(null);
        }

        // if not at end make a new engine
        if (pMzKey != null) {
            setEngine(new StableClusteringEngine(Defaults.getDefaultSimilarityChecker(), Defaults.getSimilarityThreshold()));
            setMajorMZ(pMzKey.getPrecursorMZ());
            int bin = pMzKey.getBin();
            setCurrentBin(bin);
            int group = pMzKey.getGroup();
            setCurrentGroup(group);
        }
        return true;
    }


}
