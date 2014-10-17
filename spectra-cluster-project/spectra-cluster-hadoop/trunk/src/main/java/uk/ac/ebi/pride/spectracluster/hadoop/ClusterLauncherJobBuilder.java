package uk.ac.ebi.pride.spectracluster.hadoop;

import org.systemsbiology.hadoop.*;

import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ClusterLauncherJobBuilder
 *
 * @author Steve Lewis
 * @date 29/10/13
 */
public class ClusterLauncherJobBuilder extends AbstractClusterLauncherJobBuilder {

    public static final IJobBuilderFactory FACTORY = new IJobBuilderFactory() {
        @Override
        public IJobBuilder getJobBuilder(IStreamOpener launcher, Object... otherData) {
            return new ClusterLauncherJobBuilder((ClusterLauncher) launcher);
        }
    };

    public static final Class[] DEFAULT_JOB_CLASSES =
            {
                    SpectraPeakClustererPass1.class,
                    SpectraClustererMergerOffset.class,
                    SpectraClustererMerger.class,
                    ClusterConsolidator.class
            };


    protected static Class[] makeJobClasses(ClusterLauncher launcher) {
        IParameterHolder application = launcher.getApplication();
        String steps = application.getParameter("uk.ac.ebi.pride.spectracluster.hadoop.ClusterLauncher.Steps");
        if (steps != null)
            return buildStepClasses(steps);
        else
            return DEFAULT_JOB_CLASSES;

    }

    protected static Class[] buildStepClasses(final String pSteps) {
        try {
            String[] classNames = pSteps.split(",");
            List<Class> holder = new ArrayList<Class>();

            for (int i = 0; i < classNames.length; i++) {
                String className = classNames[i].trim();
                Class<?> cls = Class.forName(className);
                cls.getMethods(); // force fuller load
                holder.add(cls);
            }
            Class[] ret = new Class[holder.size()];
            holder.toArray(ret);
            return ret;
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);

        }
    }

    public ClusterLauncherJobBuilder(ClusterLauncher launcher) {
        super(launcher, makeJobClasses(launcher));
         HadoopJob.setJarRequired(true);
    }

    /**
     * if we start at the first job - how many jobs are there in total
     *
     * @return
     */
    @Override
    public int getNumberJobs() {
        return super.getNumberJobs();
    }
}
