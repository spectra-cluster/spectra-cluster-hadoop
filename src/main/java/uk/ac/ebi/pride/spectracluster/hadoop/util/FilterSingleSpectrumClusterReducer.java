package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * FilterSingleSpectrumClusterReducer filters out single-spectrum clusters which have been
 * written out before. This class is for reducing the number of duplicated clusters produced
 * by the major peak cluster job.
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

    protected void writeOneCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
        Set<String> spectralIds = cluster.getSpectralIds();

        if (spectralIds.size() == 1) {
            String id = spectralIds.iterator().next();
            if (writtenSpectra.contains(id) || previousWrittenSpectra.contains(id))
                return; // already written
        }
        writtenSpectra.addAll(spectralIds);

        super.writeOneCluster(context, cluster);
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