package uk.ac.ebi.pride.spectracluster.hadoop;

import org.systemsbiology.hadoop.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.AbstractClusterLauncherJobBuilder
 *
 * @author Steve Lewis
 * @date 29/10/13
 */
public  class AbstractClusterLauncherJobBuilder implements IJobBuilder {



    protected final ClusterLauncher launcher;
    private int m_PassNumber = 1;
    private final Class<? extends IJobRunner>[] m_JobClasses;

    public AbstractClusterLauncherJobBuilder(ClusterLauncher launcher, Class<? extends IJobRunner>[] classes) {
        this.launcher = launcher;
        m_JobClasses = classes;
    }

    public ClusterLauncher getLauncher() {
        return launcher;
    }

    /**
     * where output directories go
     *
     * @return path name
     */
    @SuppressWarnings("UnusedDeclaration")
    public String getOutputLocation() {
        return getLauncher().getOutputLocation(getPassNumber());
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getLastOutputLocation() {
        return getLauncher().getOutputLocation(getPassNumber() - 1);
    }

    public int getPassNumber() {
        return m_PassNumber;
    }

    @SuppressWarnings("UnusedDeclaration")
    public void setPassNumber(final int pPassNumber) {
        m_PassNumber = pPassNumber;
    }

    public void incrementPassNumber() {
        m_PassNumber++;
    }


    public Class<? extends IJobRunner>[] getJobClasses() {
        return m_JobClasses;
    }

    /**
     * if we start at the first job - how many jobs are there in total
     *
     * @return
     */
    @Override
    public int getNumberJobs() {
        return getJobClasses().length;
    }

    /**
     * build the list of jobs to run
     *
     * @return
     */
    @Override
    public List<IHadoopJob> buildJobs() {

        return buildJobs(0);
    }


    public List<IHadoopJob> buildJobs(int startAtJob) {
        List<IHadoopJob> holder = new ArrayList<IHadoopJob>();
        int jobNumber = startAtJob;

        ClusterLauncher base = getLauncher();
        //noinspection ConstantConditions

        Class<? extends IJobRunner>[] jobClasses = getJobClasses();

        // special code if we start at job 0 where the data is not on hdfs
        if (startAtJob == 0) {
            String spectrumPath = base.getSpectrumPath();
            Class<? extends IJobRunner> jobClass = jobClasses[0];
            holder.add(buildJob(spectrumPath, jobClass, jobNumber));
            startAtJob++;
            jobNumber++;
        }


        for (int i = startAtJob; i < jobClasses.length; i++) {
            String job0Output = base.getOutputLocation(jobNumber);

              IHadoopJob j1 = buildJob(job0Output, jobClasses[i], jobNumber);
            holder.add(j1);
            jobNumber++;

        }
        return holder;
    }

    protected IHadoopJob buildJob(String inputFile, Class<? extends IJobRunner> mainClass, int jobNumber, String... addedArgs) {
        ClusterLauncher base = getLauncher();
        boolean buildJar = base.isBuildJar();
        HadoopJob.setJarRequired(buildJar);
        String outputLocation = base.getOutputLocation(jobNumber + 1);
        String spectrumFileName = new File(inputFile).getName();
        String inputPath = base.getRemoteBaseDirectory() + "/" + spectrumFileName;
        String jobDirectory = "jobs";
        //noinspection UnusedDeclaration
        int p = base.getRemoteHostPort();
        String[] added = base.buildJobStrings(addedArgs);


        //    if (p <= 0)
        //         throw new IllegalStateException("bad remote host port " + p);
        IHadoopJob job = HadoopJob.buildJob(
                mainClass,
                inputPath,     // data on hdfs
                jobDirectory,      // jar location
                outputLocation,
                added
        );

        String jarFile = job.getJarFile();
        if(jarFile != null)
             base.setJarFile(jarFile);

        if (base.getJarFile() != null)
            job.setJarFile(base.getJarFile());
        // reuse the only jar file
        // job.setJarFile(job.getJarFile());

        incrementPassNumber();
        return job;

    }

}
