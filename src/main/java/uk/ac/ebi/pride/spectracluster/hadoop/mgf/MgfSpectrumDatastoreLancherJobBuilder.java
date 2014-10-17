package uk.ac.ebi.pride.spectracluster.hadoop.mgf;

import org.systemsbiology.hadoop.IJobBuilder;
import org.systemsbiology.hadoop.IJobBuilderFactory;
import org.systemsbiology.hadoop.IStreamOpener;
import uk.ac.ebi.pride.spectracluster.hadoop.*;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class MgfSpectrumDatastoreLancherJobBuilder extends AbstractClusterLauncherJobBuilder {

    public static final IJobBuilderFactory FACTORY = new IJobBuilderFactory() {
        @Override
        public IJobBuilder getJobBuilder(IStreamOpener launcher, Object ... added) {
            return new MgfSpectrumDatastoreLancherJobBuilder((ClusterLauncher)launcher);
        }
    };

    public static final Class[] JOB_CLASSES = {
                    MgfSpectrumDatastoreLoadingJob.class,
    } ;

    public MgfSpectrumDatastoreLancherJobBuilder(ClusterLauncher launcher) {
        super(launcher,JOB_CLASSES);
    }

}
