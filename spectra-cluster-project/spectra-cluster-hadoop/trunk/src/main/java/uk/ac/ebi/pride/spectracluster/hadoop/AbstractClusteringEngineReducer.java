package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.IWideBinner;
import com.lordjoe.utilities.ElapsedTimer;
import org.systemsbiology.hadoop.AbstractParameterizedReducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.*;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.ClusterUtilities;
import uk.ac.ebi.pride.spectracluster.util.Defaults;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.AbstractClusteringEngineReducer
 * User: Steve
 * NOTE many methods are final to force their elimination in subclasses
 * Date: 3/28/14
 */
public abstract class AbstractClusteringEngineReducer extends AbstractParameterizedReducer {
    protected double majorPeak;
    protected int currentCharge;
    private int currentBin;
    private IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;
    private IncrementalClusteringEngineFactory factory = new IncrementalClusteringEngineFactory();
    private IIncrementalClusteringEngine engine;
    private ElapsedTimer binTime = new ElapsedTimer();
    private ElapsedTimer jobTime = new ElapsedTimer();
    // track spectra written so we don't write them twice
    private Set<String> writtenSpectra = new HashSet<String>();
    private Set<String> lastWrittenSpectra = new HashSet<String>();




    protected AbstractClusteringEngineReducer() {
    }


    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        ConfigurableProperties.configureAnalysisParameters(getApplication());
    }



    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        //    writeParseParameters(context);
        updateEngine(context, null); // write any left over clusters
        super.cleanup(context);

    }


    public final ElapsedTimer getBinTime() {
        return binTime;
    }

    @SuppressWarnings("UnusedDeclaration")
    public final ElapsedTimer getJobTime() {
        return jobTime;
    }

    public final IIncrementalClusteringEngine getEngine() {
        return engine;
    }

    public final int getCurrentBin() {
        return currentBin;
    }

    public final IWideBinner getBinner() {
        return binner;
    }

    public boolean setCurrentBin(int currentBin) {
        this.currentBin = currentBin;
        double mid = getBinner().fromBin(currentBin);
        String midStr = String.format("%10.1f", mid).trim();
        binTime.reset();
        jobTime.showElapsed("Handling bin " + currentBin + " " + midStr, System.err);
        //   if((currentBin != 149986))
        //       return false;
        return true; // use this
    }

    public final void setBinner(final IWideBinner pBinner) {
        binner = pBinner;
    }

    public void setEngine(final IIncrementalClusteringEngine pEngine) {
        engine = pEngine;
        lastWrittenSpectra.clear(); // forget
        lastWrittenSpectra.addAll(writtenSpectra); // keep one set
        writtenSpectra.clear(); // ready for next set
     }

    public final double getMajorPeak() {
        return majorPeak;
    }

    public final int getCurrentCharge() {
        return currentCharge;
    }

    public final IncrementalClusteringEngineFactory getFactory() {
        return factory;
    }

    public final void setMajorPeak(final double pMajorPeak) {
        majorPeak = pMajorPeak;
    }

    public final void setCurrentCharge(final int pCurrentCharge) {
        currentCharge = pCurrentCharge;
    }

    public final void setFactory(final IncrementalClusteringEngineFactory pFactory) {
        factory = pFactory;
    }

    /**
     * write cluster and key
     *
     * @param context  !null context
     * @param clusters !null list of clusters
     */
    protected void writeClusters(final Context context, final Collection<ICluster> clusters) throws IOException, InterruptedException {
        for (ICluster cluster : clusters) {
            writeCluster(context, cluster);
        }
    }

    /**
     * write one cluster and key
     *
     * @param context !null context
     * @param cluster !null cluster
     */
    protected void writeCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        final List<ICluster> allClusters = ClusterUtilities.findNoneFittingSpectra(cluster, engine.getSimilarityChecker(),Defaults.getRetainThreshold());
        if (!allClusters.isEmpty()) {
            for (ICluster removedCluster : allClusters) {

                // drop all spectra
                final List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
                ISpectrum[] allRemoved = clusteredSpectra.toArray(new ISpectrum[clusteredSpectra.size()]);
                cluster.removeSpectra(allRemoved);

                // and write as stand alone
                writeOneCluster(context, removedCluster);
            }

        }
        // now write the original
        writeOneCluster(context, cluster);     // nothing removed
    }

    /**
     * break up a small cluster into single spectra
     *
     * @param context
     * @param cluster
     * @throws IOException
     * @throws InterruptedException
     */
    protected final void writeAsSingleClusters(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        for (ISpectrum spc : clusteredSpectra) {
            writeOneCluster(context, ClusterUtilities.asCluster(spc));
        }
    }


    protected final void writeOneCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        String id;
        ISpectrum spc;
        switch (clusteredSpectra.size()) {
            case 0:
                return; // nothing to write
            case 1:
                spc = clusteredSpectra.get(0);
                id = spc.getId();
                if (writtenSpectra.contains(id) || lastWrittenSpectra.contains(id))
                    return; // already written
                writtenSpectra.add(id);
                break;
            case 2:
            case 3:
                if(true)
                    break; // being conservative to fiz test sets
                // todo - is this step too aggressive
                for (ISpectrum spc1 : clusteredSpectra) {
                    id = spc1.getId();
                    if (writtenSpectra.contains(id) || lastWrittenSpectra.contains(id)) {
                        writeAsSingleClusters(context, cluster);
                        return;
                    }
                    writtenSpectra.add(id);

                }
                break;
        }

        writeOneVettedCluster(context, cluster);     // nothing removed

    }

    /**
       * this version of writeCluster does all the real work
       * @param context
       * @param cluster
       * @throws IOException
       * @throws InterruptedException
       */
    protected abstract void writeOneVettedCluster(@Nonnull final Context context,@Nonnull  final ICluster cluster) throws IOException, InterruptedException;
//    {
//        ChargeMZKey key = new ChargeMZKey(cluster.getPrecursorCharge(), cluster.getPrecursorMz());
//
//        StringBuilder sb = new StringBuilder();
//        cluster.append(sb);
//        String string = sb.toString();
//
//        if (string.length() > SpectraHadoopUtilities.MIMIMUM_CLUSTER_LENGTH) {
//            writeKeyValue(key.toString(), string, context);
//            incrementBinCounters(key, context); // how big are the bins - used in next job
//        }
//    }

    // is this needed
//    protected abstract void incrementBinCounters(ChargeMZKey mzKey, Context context);
//    {
//        IWideBinner binner = Defaults.DEFAULT_WIDE_MZ_BINNER;
//        int[] bins = binner.asBins(mzKey.getPrecursorMZ());
//        //noinspection ForLoopReplaceableByForEach
//        for (int i = 0; i < bins.length; i++) {
//            int bin = bins[i];
//            SpectraHadoopUtilities.incrementPartitionCounter(context, "Bin", bin);
//
//        }
//
//    }

    /**
     * make a new engine because  either we are in a new peak or at the end (pMZKey == null
     *
     * @param context !null context
     * @param pMzKey  !null unless done
     */
    protected abstract <T> boolean updateEngine(final Context context, final T pMzKey) throws IOException, InterruptedException;
//    {
//        if (engine != null) {
//            final List<ISpectralCluster> clusters = engine.getClusters();
//            writeClusters(context, clusters);
//            engine = null;
//        }
//        // if not at end make a new engine
//        if (pMzKey != null) {
//
//            engine = factory.getIncrementalClusteringEngine(Defaults.getMajorPeakMZWindowSize());
//         //   majorPeak = pMzKey.getPeakMZ();
//         //   currentCharge = pMzKey.getCharge();
//            return setFromKey(pMzKey) ;
//        }
//    }
//
//    protected abstract <T> boolean setFromKey(T Key) ;


}
