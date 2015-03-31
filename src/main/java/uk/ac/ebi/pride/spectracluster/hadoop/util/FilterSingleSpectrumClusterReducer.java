package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.IKeyable;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ClusterWritableReducer is an abstract class that provide output functionality for cluster
 *
 * @author Rui Wang
 * @version $Id$
 */
public abstract class FilterSingleSpectrumClusterReducer extends ClusterWritableReducer {
    /**
     * Spectrum ids have been written in the current reduce step
     */
    private final Set<String> writtenSpectra = new HashSet<String>();
    /**
     * Spectrum ids have been written in the previous reduce step
     */
    private final Set<String> previousWrittenSpectra = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        clearCache();
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        clearCache();
        super.cleanup(context);
    }

    protected void writeVettedCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        Set<String> spectralIds = cluster.getSpectralIds();

        if (spectralIds.size() == 1) {
            String id = spectralIds.iterator().next();
            if (writtenSpectra.contains(id) || previousWrittenSpectra.contains(id))
                return; // already written
        }
        writtenSpectra.addAll(spectralIds);

        super.writeCluster(context, cluster);
    }

    /**
     * Default cluster output key is MZKey, this key is used for the reducer output
     * @param cluster   intput cluster
     * @return  output key
     */
    protected IKeyable makeClusterOutputKey(ICluster cluster) {
        return new MZKey(cluster.getPrecursorMz());
    }

    protected void updateCache() {
        previousWrittenSpectra.clear();
        previousWrittenSpectra.addAll(writtenSpectra); // keep one set
        writtenSpectra.clear(); // ready for next set
    }

    protected void clearCache() {
        previousWrittenSpectra.clear();
        writtenSpectra.clear();
    }
}