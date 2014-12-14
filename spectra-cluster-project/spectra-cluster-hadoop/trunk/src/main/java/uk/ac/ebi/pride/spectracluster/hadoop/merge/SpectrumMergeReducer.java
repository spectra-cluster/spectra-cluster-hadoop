package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.engine.IIncrementalClusteringEngine;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.AbstractClusterReducer;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class SpectrumMergeReducer extends AbstractClusterReducer {

    private double spectrumMergeWindowSize = ClusterHadoopDefaults.getSpectrumMergeMZWindowSize();
    private final Set<String> writtenSpectrumIdPerBin = new HashSet<String>();
    private final Set<String> seenSpectrumIdPerBin = new HashSet<String>();
    private IWideBinner binner = ClusterHadoopDefaults.getBinner();
    private int currentBin;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        boolean offsetBins = context.getConfiguration().getBoolean("pride.cluster.offset.bins", false);
        if (offsetBins) {
            IWideBinner offSetHalf = (IWideBinner) getBinner().offSetHalf();
            setBinner(offSetHalf);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numOfValues = 0;

        BinMZKey binMZKey = new BinMZKey(key.toString());

        if (binMZKey.getBin() != getCurrentBin() || getEngine() == null) {
            updateEngine(context, binMZKey);
        }

        for (Text value : values) {
            // increment number of values
            ++numOfValues;

            // report progress to avoid timeout
            if ((numOfValues % 50) == 0)
                context.progress();

            // parse cluster
            String val = value.toString();
            ICluster cluster = IOUtilities.parseClusterFromCGFString(val);

            // ignore single spectrum cluster which have been seen before
            if (cluster.getClusteredSpectraCount() == 1 && seenSpectrumIdPerBin.contains(cluster.getClusteredSpectra().get(0).getId())) {
                continue;
            }

            seenSpectrumIdPerBin.addAll(cluster.getSpectralIds());

            Collection<ICluster> removedClusters = getEngine().addClusterIncremental(cluster);
            if (!removedClusters.isEmpty()) {
                writeClusters(context, removedClusters);
            }
        }
    }

    protected <T> void updateEngine(Context context, T binMZKey) throws IOException, InterruptedException {
        if (getEngine() != null) {
            Collection<ICluster> clusters = getEngine().getClusters();
            writeClusters(context, clusters);
            setEngine(null);
        }

        if (binMZKey != null) {
            setEngine(getEngineFactory().getIncrementalClusteringEngine((float) getSpectrumMergeWindowSize()));
            setCurrentBin(((BinMZKey) binMZKey).getBin());
        }
    }

    @Override
    protected void writeOneVettedCluster(Context context, ICluster cluster) throws IOException, InterruptedException {
        /**
         * is a duplicate  so ignore
         */
        trackDuplicates(context, cluster);

        float precursorMz = cluster.getPrecursorMz();
        IWideBinner binner = getBinner();
        int bin = binner.asBin(precursorMz);

        // you can merge clusters outside the current bin but not write them
        if (bin != getCurrentBin()) {
            // track when this happens
            String offString = bin > getCurrentBin() ? "above" : "below";
            Counter counter = context.getCounter("OutsideBin", offString);
            counter.increment(1);
            return;
        }

        super.writeOneVettedCluster(context, cluster);
    }

    /**
     * todo: review this method
     *
     * @param context
     * @param cluster
     * @return
     */
    private void trackDuplicates(Context context, ICluster cluster) {

        /**
         * this entire section is here to track duplicates and stop writing single spectra when
         * they are already clustered
         */
        int clusteredSpectraCount = cluster.getClusteredSpectraCount();
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        if (clusteredSpectraCount == 1) {
            ISpectrum onlySpectrum = clusteredSpectra.get(0);
            if (writtenSpectrumIdPerBin.contains(onlySpectrum.getId())) {
                Counter counter = context.getCounter("Duplicates", "attempt_single");
                counter.increment(1);
            }
        } else {
            for (ISpectrum spc : clusteredSpectra) {
                Counter counter = context.getCounter("Duplicates", "add_spectrum");
                counter.increment(1);

                String id = spc.getId();
                if (!writtenSpectrumIdPerBin.contains(id)) {
                    writtenSpectrumIdPerBin.add(id); // track when written
                } else {
                    counter = context.getCounter("Duplicates", "add_duplicate");
                    counter.increment(1);
                }
            }
        }
    }

    public IWideBinner getBinner() {
        return binner;
    }

    public void setBinner(IWideBinner binner) {
        this.binner = binner;
    }

    public int getCurrentBin() {
        return currentBin;
    }

    public void setCurrentBin(int currentBin) {
        this.currentBin = currentBin;
    }

    public void setEngine(IIncrementalClusteringEngine engine) {
        super.setEngine(engine);
        this.writtenSpectrumIdPerBin.clear();
        this.seenSpectrumIdPerBin.clear();
    }

    public double getSpectrumMergeWindowSize() {
        return spectrumMergeWindowSize;
    }

    public void setSpectrumMergeWindowSize(double spectrumMergeWindowSize) {
        this.spectrumMergeWindowSize = spectrumMergeWindowSize;
    }
}
