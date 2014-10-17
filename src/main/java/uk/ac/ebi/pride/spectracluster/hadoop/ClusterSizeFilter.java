package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.utilities.TypedPredicate;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;

import javax.annotation.Nonnull;

/**
 * uk.ac.ebi.pride.spectracluster.cluster.ClusterSizeFilter
 * filter clustera on size supporting either  minimum only or minumum and maximum size
 * User: Steve
 * Date: 9/25/13
 */
@SuppressWarnings("UnusedDeclaration")
public class ClusterSizeFilter implements TypedPredicate<ICluster> {

    private final int minimumSize;
    private final int maximumSize;

    public ClusterSizeFilter(final int pMinimumSize) {
        this(pMinimumSize, Integer.MAX_VALUE); // no upper limit
    }

    public ClusterSizeFilter(final int pMinimumSize, final int maxSize) {
        minimumSize = pMinimumSize;
        maximumSize = maxSize;
    }

    public int getMinimumSize() {
        return minimumSize;
    }

    public int getMaximumSize() {
        return maximumSize;
    }

    /**
     * @param otherdata - implementation specific and usually blank
     * @return what the implementation does
     */
    @Override
    public boolean apply(@Nonnull final ICluster cluster, final Object... otherdata) {
        int clusteredSpectraCount = cluster.getClusteredSpectraCount();
        if (clusteredSpectraCount < getMinimumSize())
            return false;
        //noinspection RedundantIfStatement
        if (clusteredSpectraCount > getMaximumSize())
            return false;
        return true;
    }
}
