package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.utilities.TypedPredicate;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.IClusterAppender;

/**
 * uk.ac.ebi.pride.spectracluster.io.FilteredClusterAppender
 * build with one appender and one filter - allend if the predicate says to do so
 * User: Steve
 * Date: 9/25/13
 */
public class FilteredClusterAppender implements IClusterAppender {
    private final IClusterAppender clusterAppender;
    private final TypedPredicate<ICluster> m_AppendIf;

    public FilteredClusterAppender(final IClusterAppender wrapped, TypedPredicate<ICluster> appendIf) {
        clusterAppender = wrapped;
        m_AppendIf = appendIf;
    }

    /**
     * @param out       !null open appendale
     * @param data      !null cluster
     * @param otherData any other data - implementation specific and usually blank
     * @return true if anything was appended otherwise false
     */
    @Override
    public void appendCluster(final Appendable out, final ICluster data, final Object... otherData) {
        //noinspection  SimplifiableIfStatement
        if (m_AppendIf.apply(data, otherData)) {
            clusterAppender.appendCluster(out, data, otherData);
        }
    }

    /**
     * add whatever happens at the start
     *
     * @param out       !null open appendale
     * @param otherData any other data - implementation specific and usually blank
     * @return true if anything was appended otherwise false
     */
    @Override
    public void appendStart(final Appendable out, final Object... otherData) {
        clusterAppender.appendStart(out, otherData);
    }

    /**
     * add whatever happens at the end
     *
     * @param out       !null open appendale
     * @param otherData any other data - implementation specific and usually blank
     * @return true if anything was appended otherwise false
     */
    @Override
    public void appendEnd(final Appendable out, final Object... otherData) {
        clusterAppender.appendEnd(out, otherData);
    }
}
