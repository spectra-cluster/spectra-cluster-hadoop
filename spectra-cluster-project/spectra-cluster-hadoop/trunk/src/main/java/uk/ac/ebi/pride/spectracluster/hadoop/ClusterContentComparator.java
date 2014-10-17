package uk.ac.ebi.pride.spectracluster.hadoop;

import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.util.comparator.ClusterComparator;

/**
 * Compare spectrum ids in addition to ClusterComparator
 *
 * @author Rui Wang
 * @version $Id$
 */
public class ClusterContentComparator extends ClusterComparator {
    public static ClusterContentComparator INSTANCE = new ClusterContentComparator();

    private ClusterContentComparator() {
    }

    @Override
    public int compare(ICluster o1, ICluster o2) {
        String s1 = SpectrumInCluster.listClusterIds(o1);
        String s2 = SpectrumInCluster.listClusterIds(o2);
        if (!s1.equals(s2))
            return s1.compareTo(s2);  // differrent spectra
        // same spectra
        return super.compare(o1, o2);
    }
}
