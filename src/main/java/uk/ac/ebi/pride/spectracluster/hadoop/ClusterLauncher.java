package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.utilities.*;
import org.apache.hadoop.conf.Configuration;
import org.systemsbiology.common.IFileSystem;
import org.systemsbiology.hadoop.*;
import org.systemsbiology.remotecontrol.*;
import org.systemsbiology.xml.XMLUtilities;

import java.io.*;
import java.util.*;
import java.util.prefs.Preferences;

import static org.systemsbiology.hadoop.HadoopUtilities.*;


// add lots of constants from this class

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ClusterLauncher
 * Launches a hadoop job on the cluster
 * specialized for spectral clustering
 * User: steven
 * Date: Jan 5, 2011
 * Singleton representing a JXTandem job -
 * This has the program main
 */
public class ClusterLauncher implements IStreamOpener { //extends AbstractParameterHolder implements IParameterHolder {


    // for development you can skip the first jobs ot work on issues in the second
    private static int gDefaultStartAtJob = 0; // 0; // 1;

    public static int getDefaultStartAtJob() {
        return gDefaultStartAtJob;
    }

    public static void setDefaultStartAtJob(int gDefaultStartAtJob) {
        ClusterLauncher.gDefaultStartAtJob = gDefaultStartAtJob;
    }

    // files larger than this are NEVER copied from the remote cluster
    public static final int MAX_DATA_TO_COPY = 1 * 1024 * 1024 * 1024;

    // files smaller than this are always written to the remmote cluster
    public static final int SMALL_FILE_LENGTH = 1000;


    public static final String CLUSTER_VERSION = "1.0.0";

    public static final String INPUT_PATH_PROPERTY = "input_path";
    public static final String START_AT_JOB_PROPERTY = "start-at-job";
    public static final String DATA_ON_PATH_PROPERTY = "data_on_cluster";
    public static final String DO_NOT_COPY_FILES_PROPERTY = "org.systemsbiology.xtandem.DoNotCopyFilesToLocalMachine";
    public static final String INPUT_FILES_PROPERTY = "org.systemsbiology.xtandem.InputFiles";

    public static final int MAX_DISPLAY_LENGTH = 4 * 1000 * 1000;

    private static boolean directoryInputHandled;

    public static boolean isDirectoryInputHandled() {
        return directoryInputHandled;
    }

    public static void setDirectoryInputHandled(boolean directoryInputHandled) {
        ClusterLauncher.directoryInputHandled = directoryInputHandled;
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    //  public static final int NUMBER_STAGES = TOTAL_STAGES - START_AT_JOB;

    private static HadoopMajorVersion g_RunningHadoopVersion = HadoopMajorVersion.CURRENT_VERSION;

    /**
     * you can try forcing code to run against a lower Hadoop version
     *
     * @return
     */
    @SuppressWarnings("UnusedDeclaration")
    public static HadoopMajorVersion getRunningHadoopVersion() {
        return g_RunningHadoopVersion;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void setRunningHadoopVersion(HadoopMajorVersion runningHadoopVersion) {
        if (HadoopMajorVersion.CURRENT_VERSION == HadoopMajorVersion.Version0 &&
                runningHadoopVersion != HadoopMajorVersion.Version0)
            throw new IllegalArgumentException("Cannot run against higher than the build version");
        if (HadoopMajorVersion.CURRENT_VERSION == HadoopMajorVersion.Version1 &&
                runningHadoopVersion == HadoopMajorVersion.Version2)
            throw new IllegalArgumentException("Cannot run against higher than the build version");
        g_RunningHadoopVersion = runningHadoopVersion;
    }


    private static String gPassedJarFile;

    public static String getPassedJarFile() {
        return gPassedJarFile;
    }

    public static void setPassedJarFile(final String pPassedJarFile) {
        gPassedJarFile = pPassedJarFile;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void logMessage(String message) {
        XMLUtilities.outputLine(message);
    }


    @SuppressWarnings("UnusedDeclaration")
    protected static File getOutputFile(IParameterHolder main, String key) {
        File ret = new File(main.getParameter(key));
        File parentFile = ret.getParentFile();
        if (parentFile != null) {
            if (!parentFile.exists())
                //noinspection ResultOfMethodCallIgnored
                parentFile.mkdirs();
            else {
                if (parentFile.isFile())
                    throw new IllegalArgumentException("write output file into file  " + ret.getName());
            }
        }
        if ((parentFile != null && (!parentFile.exists() || !parentFile.canWrite())))
            throw new IllegalArgumentException("cannot access output file " + ret.getName());
        if (ret.exists() && !ret.canWrite())
            throw new IllegalArgumentException("cannot rewrite output file file " + ret.getName());

        return ret;
    }

    public static final String USER_DIR = System.getProperty("user.dir").replace("\\", "/");


    private SpectraHadoopMain m_Application;

    private final RunStatistics m_Statistics = new RunStatistics();
    private String m_RemoteBaseDirectory;
    private String m_LocalBaseDirectory = USER_DIR;
    private String m_JarFile;
    private String m_OutputFileName;
    private String m_InputFiles;
    private IJobBuilder m_Builder;
    private int m_StartAtJob = getDefaultStartAtJob();

    private final DelegatingFileStreamOpener m_Openers = new DelegatingFileStreamOpener();
    private boolean m_BuildJar = true;
    private final Map<String, String> m_PerformanceParameters = new HashMap<String, String>();
    private IFileSystem m_Accessor;

    // read from list path, default parameters as notes

    @SuppressWarnings("UnusedDeclaration")
    public ClusterLauncher(final File pTaskFile) {

        setParameter("TaskFile", pTaskFile.getAbsolutePath());
        //    Protein.resetNextId();
        initOpeners();
        try {
            handleInputs(new FileInputStream(pTaskFile), new Configuration());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        setBuilder(new ClusterLauncherJobBuilder(this));
    }

    @SuppressWarnings("UnusedDeclaration")
    public ClusterLauncher(final InputStream is, Configuration cfg) {
        setParameter("TaskFile", null);
        ///     Protein.resetNextId();
        initOpeners();

        handleInputs(is, cfg);

        this.m_RemoteBaseDirectory = getPassedBaseDirctory();
        if (isTaskLocal())
            this.m_LocalBaseDirectory = getRemoteBaseDirectory();
        //     if (gInstance != null)
        //        throw new IllegalStateException("Only one XTandemMain allowed");

        setBuilder(new ClusterLauncherJobBuilder(this));
    }

    public IJobBuilder getBuilder() {
        return m_Builder;
    }

    public void setBuilder(IJobBuilder builder) {
        m_Builder = builder;
    }

    /**
     * set a parameter value
     *
     * @param key   !null key
     * @param value !null value
     */
    public void setParameter(String key, String value) {
        getStatistics().setData(key, value);
    }

    /**
     * return a parameter configured in  default parameters
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public String getParameter(String key) {
        return getStatistics().getData(key);
    }


    public RunStatistics getStatistics() {
        return m_Statistics;
    }


    @SuppressWarnings("UnusedDeclaration")
    public void setPerformanceParameter(String key, String value) {
        m_PerformanceParameters.put(key, value);
    }

    public String getInputFiles() {
        return m_InputFiles;
    }

    public void setInputFiles(final String inputFiles) {
        m_InputFiles = inputFiles;
    }

    public boolean isDataOnCluster() {
        SpectraHadoopMain application = getApplication();
        return application.getBooleanParameter(DATA_ON_PATH_PROPERTY, false);
    }

    public int getNumberStages() {
        return getBuilder().getNumberJobs();
    }


    public int getStartAtJob() {
        return m_StartAtJob;
    }


    public void setStartAtJob(int startAtJob) {
        m_StartAtJob = startAtJob;
    }

    /**
     * return a parameter configured in  default parameters
     *
     * @param key !null key
     * @return possibly null parameter
     */
    @SuppressWarnings("UnusedDeclaration")
    public String getPerformanceParameter(String key) {
        return m_PerformanceParameters.get(key);
    }

    /**
     * return a parameter configured in  rgw tandem
     *
     * @param key !null key
     * @return possibly null parameter
     */
    public String getTandemParameter(String key) {
        return getApplication().getParameter(key);
    }

    @SuppressWarnings("UnusedDeclaration")
    public String[] getPerformanceKeys() {
        Set<String> strings = m_PerformanceParameters.keySet();
        String[] ret = strings.toArray(new String[strings.size()]);
        Arrays.sort(ret);
        return ret;
    }


    public boolean isBuildJar() {
        if (getJarFile() != null)
            return false;
        return m_BuildJar;
    }

    public void setBuildJar(final boolean pBuildJar) {
        m_BuildJar = pBuildJar;
    }

    public String getOutputLocationBase() {
        return getParameter("OutputLocationBase");
    }

    public void setOutputLocationBase(final String pOutputLocationBase) {
        setParameter("OutputLocationBase", pOutputLocationBase);
    }

    public String getOutputFileName() {
        if (m_OutputFileName == null) {
            SpectraHadoopMain application = getApplication();
            m_OutputFileName = buildDefaultFileName(application);
        }

        return m_OutputFileName;
    }

    public static String buildDefaultFileName(IParameterHolder pParameters) {

        String name = pParameters.getParameter("output, path");
        // little hack to separate real tandem and hydra results
        if (name != null)
            name = name.replace(".tandem.xml", ".hydra.xml");
        if ("full_tandem_output_path".equals(name)) {
            System.err.println("output matches input file");
            return name;
        }

        if (name == null)
            name = "MyOutputFile.txt";
        return name;
    }


    /**
     * where output directories go
     *
     * @param passNumber job number - guarantees uniqueness
     * @return path name
     */
    public String getOutputLocation(int passNumber) {
        return getOutputLocationBase() + passNumber;
    }

    /**
     * add new ways to open files
     */
    protected void initOpeners() {
        addOpener(new FileStreamOpener());
        addOpener(new StreamOpeners.ResourceStreamOpener(StreamOpeners.class));
        for (IStreamOpener opener : HadoopUtilities.getPreloadOpeners())
            addOpener(opener);
    }


    /**
     * open a file from a string
     *
     * @param fileName  string representing the file
     * @param otherData any other required data
     * @return possibly null stream
     */
    @Override
    public InputStream open(String fileName, Object... otherData) {
        return m_Openers.open(fileName, otherData);
    }

    public void addOpener(IStreamOpener opener) {
        m_Openers.addOpener(opener);
    }


    public ElapsedTimer getElapsed() {
        return getStatistics().getTotalTime();
    }

    public String getJarFile() {
        return m_JarFile;
    }

    public void setJarFile(final String pJarFile) {
        m_JarFile = pJarFile;
    }

    /**
     * parse the initial file and get run parameters
     *
     * @param is
     */
    public void handleInputs(final InputStream is, Configuration cfg) {

        m_Application = SpectraHadoopMain.getInstance(is, cfg);

        Properties properties = HadoopUtilities.getHadoopProperties();
        for (String key : properties.stringPropertyNames()) {
            String val = properties.getProperty(key);
            m_Application.setParameter(key, val);
        }
        Integer startAtJob = m_Application.getIntParameter(START_AT_JOB_PROPERTY, 0);
        setDefaultStartAtJob(startAtJob);
        setStartAtJob(startAtJob);


    }

    protected String refinePath(String originalPath) {
        String remoteBaseDirectory = getRemoteBaseDirectory();
        if (remoteBaseDirectory.startsWith("/"))
            return originalPath; // already absolute
        String adjustedUserDir = USER_DIR;
        int clnIdx = USER_DIR.indexOf(":");
        if (clnIdx > -1) {
            adjustedUserDir = adjustedUserDir.substring(clnIdx + 1); // drop things like C:/
        }
        if (!adjustedUserDir.equals(remoteBaseDirectory))
            return remoteBaseDirectory + "/" + originalPath;
        else
            return originalPath;
    }

    public String getSpectrumPath() {
        String tandemParameter = getTandemParameter(INPUT_PATH_PROPERTY);
        tandemParameter = refinePath(tandemParameter);

        return tandemParameter;
    }


    public String getRemoteBaseDirectory() {
        if (m_RemoteBaseDirectory != null)
            return m_RemoteBaseDirectory;
        else
            return m_LocalBaseDirectory;
    }

    public void setRemoteBaseDirectory(final String pRemoteBaseDirectory) {
        m_RemoteBaseDirectory = pRemoteBaseDirectory;
        HadoopUtilities.setDefaultPath(pRemoteBaseDirectory);
    }

    public String getRemoteHost() {
        return getParameter("RemoteHost");
    }

    public void setRemoteHost(final String pRemoteHost) {
        setParameter("RemoteHost", pRemoteHost);
    }

    public int getRemoteHostPort() {
        String remoteHostPort = getParameter("RemoteHostPort");
        if (remoteHostPort == null)
            return 0;
        return Integer.parseInt(remoteHostPort);
    }

    public void setRemoteHostPort(final int pRemoteHostPort) {
        setParameter("RemoteHostPort", Integer.toString(pRemoteHostPort));
    }

    public String getParamsPath() {
        return getParameter("ParamsPath");
    }

    public String getTaskParamsPath() {
        String rbase = getRemoteBaseDirectory();
        String paramsPath = getParamsPath();
        if (rbase != null) {
            return rbase + "/" + new File(paramsPath).getName();

        }
        return paramsPath;
    }

    public void setParamsPath(String pParamsPath) {
//        String rbase = getRemoteBaseDirectory();
//        if (rbase != null) {
//            pParamsPath = rbase + "/" + pParamsPath;
//
//        }
        setParameter("ParamsPath", pParamsPath);
    }


    /*
    * modify checks the input parameters for known parameters that are use to modify
    * a protein sequence. these parameters are stored in the m_pScore member object's
    * msequenceutilities member object
    */


    public static void usage() {
        XMLUtilities.outputLine("Usage - JXTandem config=<confFile> params=<inputfile> <forceDatabaseRebuild> <buildDatabaseOnly>");
    }

    public static void usage2() {
        XMLUtilities.outputLine("Usage - JXTandem <inputfile>");
    }

    public static void usage(String filename) {
        XMLUtilities.outputLine("Usage - JXTandem <inputfile> cannot read file " + filename + " in directory " +
                System.getProperty("user.dir"));
    }

    public IFileSystem getAccessor() {
        return m_Accessor;
    }

    public void setAccessor(final IFileSystem pAccessor) {
        m_Accessor = pAccessor;
    }

    /**
     * @param hdfsPath
     * @param localFile
     * @return !null file
     * @throws IllegalArgumentException if the remote file does not exist
     */
    public File readRemoteFile(String hdfsPath, String localFile) throws IllegalArgumentException {
//        int p = getRemoteHostPort();
//        if (p <= 0)
//            throw new IllegalStateException("bad remote host port " + p);
//
        IFileSystem acc = getAccessor();
        if (!acc.exists(hdfsPath)) {
            String s = "remote file " + hdfsPath + " does not exist";
            System.out.println(s);
            return null;
            // throw new IllegalArgumentException(s);
        } else
            XMLUtilities.outputLine("Copying file " + hdfsPath + " to " + localFile);
        File out = new File(localFile);
        acc.copyFromFileSystem(hdfsPath, out);
        //      String remotepath = getOutputLocation(3) + "/" + fileName;
        return out;
    }


    protected int guaranteeRemoteDirectory(String baseDir, File file) {
        IFileSystem accessor = getAccessor();
        String remotepath = baseDir + "/" + file.getName();
        accessor.guaranteeDirectory(remotepath);
        File[] files = file.listFiles();
        int ret = 0;
        //noinspection ForLoopReplaceableByForEach,ConstantConditions
        for (int i = 0; i < files.length; i++) {
            File file1 = files[i];
            if (file1.isFile()) {
                String path = remotepath + "/" + file1.getName();
                XMLUtilities.outputLine("Writing to remote " + file1 + " " + file1.length() / 1000 + "kb");
                accessor.writeToFileSystem(path, file1);
                ret++;
            } else {
                ret += guaranteeRemoteDirectory(remotepath, file);
            }
        }
        return ret;
    }

    protected void handleCounters(IHadoopJob job) {
        Map<String, Long> counters;
        counters = job.getAllCounterValues();
        for (String key : counters.keySet()) {
            if (HadoopUtilities.isCounterHadoopInternal(key))
                continue;
            //Counter c = counters.get(key);
            String value = Long.toString(counters.get(key));
            setParameter(key, value);
        }
        //  HadoopUtilities.showAllCounters(counters);

    }

    public static final String CLUSTERING_JOB_NAME = "Total Clustering";

    public void runJobs(IHadoopController launcher) {

        XMLUtilities.outputLine("Starting Job");
        guaranteeRemoteFiles();

        ElapsedTimer elapsed = getElapsed();

        RunStatistics statistics = getStatistics();

        statistics.startJob(CLUSTERING_JOB_NAME);
        // remember the input file foe a final report
        String parameter = getSpectrumPath();
        if (parameter != null)
            statistics.setData("Input file", parameter);
        buildInputFilesString(parameter);

        //noinspection UnusedAssignment
        boolean ret;

        int startAtJob = getStartAtJob();
        List<IHadoopJob> jobs = buildJobs(startAtJob);

        for (IHadoopJob job : jobs) {
            String jobName = job.getName();
            statistics.startJob(jobName);
            ret = launcher.runJob(job);
            if (!ret) {
                throw new IllegalStateException(jobName + "  job failed");
            }
            handleCounters(job);
            statistics.endJob(jobName);

            elapsed.showElapsed("Finished " + jobName);
            elapsed.reset();

        }


        //noinspection UnusedAssignment
        String property = HadoopUtilities.getProperty(DELETE_OUTPUT_DIRECTORIES_PROPERTY);

        property = null;

        //noinspection ConstantConditions
        if ("true".equalsIgnoreCase(property))
            deleteRemoteIntermediateDirectories();

        statistics.endJob(CLUSTERING_JOB_NAME);

        // runJob(hc);
    }

    protected void buildInputFilesString(final String inputFileName) {
        File input = new File(inputFileName);
        if (input.exists()) {
            if (input.isFile() || isDirectoryInputHandled()) {
                setInputFiles(input.getName());
                return;
            }
            if (input.isDirectory()) {
                String[] subfiles = input.list();
                if (subfiles == null)
                    return;
                StringBuilder sb = new StringBuilder();
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < subfiles.length; i++) {
                    String subfile = subfiles[i];
                    if (sb.length() > 0)
                        sb.append(",");
                    sb.append(subfile);
                }
                String inputFiles = sb.toString();
                getApplication().setParameter(INPUT_FILES_PROPERTY, inputFiles);
                setInputFiles(inputFiles);
            }
        }
    }

    public static final int MAX_JOBS = 6;

    protected void deleteRemoteIntermediateDirectories() {
        IFileSystem accessor = getAccessor();
        for (int i = 0; i < MAX_JOBS; i++) {
            String outputLocation = getOutputLocation(i);
            accessor.expunge(outputLocation);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void deleteLocalIntermediateDirectories() {
        for (int i = 0; i < MAX_JOBS; i++) {
            String outputLocation = getOutputLocation(i);
            FileUtilities.expungeDirectory(outputLocation);
        }
    }

    /**
     * copy files to hte file system designated
     *
     * @param pAccessor
     * @param pRbase
     */
    protected void guaranteeAccessibleFiles(final String pRbase) {
        IFileSystem pAccessor = getAccessor();

        XMLUtilities.outputLine("Finding Remote Directory");
        pAccessor.guaranteeDirectory(pRbase);
        XMLUtilities.outputLine("Writing Params");
        writeRemoteParamsFile(pRbase);

        // guaranteeRemoteCopy(accessor, m_TaxonomyName);
        XMLUtilities.outputLine("Writing Spectral Data");
        String spectrumPath = getSpectrumPath();

        File spectrumFile = new File(spectrumPath);
        // maybe we need to copy to the remote system or maybe it iw already there
        if (spectrumFile.exists()) {
            if (isDataOnCluster())
                return; // already there
            if (spectrumFile.isDirectory()) {
                File[] files = spectrumFile.listFiles();
                if (files == null)
                    throw new IllegalStateException("Spectrum path is a directory and is empty");
                // copy all files in directory
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    guaranteeRemoteFilePath(file, pRbase + "/" + spectrumFile.getName());
                }
            } else {
                guaranteeRemoteFilePath(spectrumFile, pRbase);
            }
        } else {
            guaranteExistanceofRemoteFile(spectrumFile, pRbase, "the Spectrum file designated by \"input_path\" ");
        }
    }

    protected int guaranteExistanceofRemoteFile(final File pFile1, final String pRemotePath, String message) {
        final IFileSystem accessor = getAccessor();
        if (pFile1.isDirectory()) {
            //noinspection UnnecessaryLocalVariable
            int ret = guaranteeRemoteDirectory(pRemotePath, pFile1);
            return ret;
        }
        String remotepath = pRemotePath + "/" + XMLUtilities.asLocalFile(pFile1.getAbsolutePath());
        // if it is small then always make a copy
        if (accessor.exists(remotepath)) {
            return 2;
        }
        throw new IllegalStateException(message + "  " + remotepath + "does not exist");
    }

    protected int guaranteeRemoteFilePath(final File pFile1, final String pRemotePath) {
        final IFileSystem accessor = getAccessor();
        if (pFile1.isDirectory()) {
            //noinspection UnnecessaryLocalVariable
            int ret = guaranteeRemoteDirectory(pRemotePath, pFile1);
            return ret;
        }
        if (!pFile1.canRead())
            return 0;

        long length = pFile1.length();

        String remotepath = pRemotePath + "/" + XMLUtilities.asLocalFile(pFile1.getAbsolutePath());
        // if it is small then always make a copy
        if (length < SMALL_FILE_LENGTH && length > 0) {
            accessor.deleteFile(remotepath);
            XMLUtilities.outputLine("Writing to remote " + pFile1 + " " + pFile1.length() / 1000 + "kb");
            accessor.guaranteeFile(remotepath, pFile1);
            return 1;
        } else {
            if (accessor.exists(remotepath)) {
                long existingLength = accessor.fileLength(remotepath);
                // for very large data files we will tolerate a lot of error to avoid recopying them
                //noinspection NumericOverflow

                if ((existingLength == length)
                        || (existingLength > MAX_DATA_TO_COPY)
                        ) {
                    return 1;
                }
            }
            accessor.deleteFile(remotepath);
            accessor.guaranteeFile(remotepath, pFile1);
            return 1;
        }
    }

    protected void writeRemoteParamsFile(final String pRbase) {
        final IFileSystem pAccessor = getAccessor();
        String paramsPath = getParamsPath();
        File f = new File(paramsPath);
        String remotepath = pRbase + "/" + f.getName();

        String newParams = buildNewParamsFile(f);


        pAccessor.writeToFileSystem(remotepath, newParams);

//        StringWriter sw = new StringWriter();
//        PrintWriter out = new PrintWriter(sw);
//        writeAdjustedParameters(out);
//
//        pAccessor.writeToFileSystem(remotepath, sw.toString());
    }

    public static final String[] EXCLUDED_PROPERTIES = {
            "io.sort.mb", "java.net.preferIPv4Stack", "io.sort.factor",
    };

    public static final Set<String> EXCLUDED_PROPERTY_SET = new HashSet<String>(Arrays.asList(EXCLUDED_PROPERTIES));

    /**
     * add params from Launcher properties into the version of tandem.xml which is uploaded
     *
     * @param f !null file with tamdem.xml
     * @return String with new content
     */
    protected String buildNewParamsFile(File f) {
        String[] paramLines = FileUtilities.readInAllLines(f);

        String lastLine = "";
        StringBuilder sb = new StringBuilder();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < paramLines.length; i++) {
            String paramLine = paramLines[i].trim();
            if (paramLine.length() == 0)
                continue; // iognore blank lines
            if (paramLine.contains("</bioml>")) {
                lastLine = paramLine;
                break;
            }
            sb.append(paramLine);
            sb.append("\n");

        }
        Properties addedProps = HadoopUtilities.getHadoopProperties();
        for (String key : addedProps.stringPropertyNames()) {
            if (EXCLUDED_PROPERTY_SET.contains(key))
                continue;
            String paramLine = "<note type=\"input\" label=\"" + key + "\">" + addedProps.getProperty(key) + "</note>";
            sb.append(paramLine);
            sb.append("\n");
        }

        sb.append(lastLine);
        sb.append("\n");
        return sb.toString();
    }


    public void guaranteeRemoteFiles() {
        boolean isRemote = !isTaskLocal();
        // if running locally files need to be there
        if (isRemote) {
            String host = RemoteUtilities.getHost(); // "192.168.244.128"; // "hadoop1";
            int port = RemoteUtilities.getPort();
            IFileSystem accessor = HDFSAccessor.getFileSystem(host, port);
            setAccessor(accessor);

            String rbase = getRemoteBaseDirectory();

            String udir = System.getProperty("user.dir");
            // running on local
            if (new File(udir).equals(new File(rbase)))
                return;
            guaranteeAccessibleFiles(rbase);
        }
    }


    public SpectraHadoopMain getApplication() {
        return m_Application;
    }


    /**
     * build definitions to pass to Hadoop jobs
     *
     * @param addedDefinitions all strings o f the form name=value - will be preceeded with a -D
     * @return
     */
    protected String[] buildJobStrings(String... addedDefinitions) {
        String taskParamsPath = getTaskParamsPath();
        String remoteBaseDirectory = getRemoteBaseDirectory();
        String remoteHost = getRemoteHost();
        int p = getRemoteHostPort();
        List<String> holder = new ArrayList<String>();
        holder.add("-D");
        holder.add(DefaultParameterHolder.PARAMS_KEY + "=" + taskParamsPath);
        holder.add("-D");
        holder.add(DefaultParameterHolder.PATH_KEY + "=" + remoteBaseDirectory);
        holder.add("-D");
        holder.add(DefaultParameterHolder.HOST_KEY + "=" + remoteHost);
        holder.add("-D");
        holder.add(DefaultParameterHolder.HOST_PORT_KEY + "=" + p);
//        Properties properties = HadoopUtilities.getHadoopProperties();
//        for (String key : properties.stringPropertyNames()) {
//            holder.add("-D");
//            String val = properties.getProperty(key);
//            holder.add(key + "=" + val);
//        }

        String inputFiles = getInputFiles();
        if (inputFiles != null) {
            holder.add("-D");
            holder.add(ClusterLauncher.INPUT_FILES_PROPERTY + "=" + inputFiles);

        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < addedDefinitions.length; i++) {
            String addedDefinition = addedDefinitions[i];
            if (addedDefinition.contains("="))
                holder.add("-D");
            holder.add(addedDefinition);

        }
        String[] ret = new String[holder.size()];
        holder.toArray(ret);
        return ret;
    }

    protected List<IHadoopJob> buildJobs(int startAtJob) {
        IJobBuilder builder = getBuilder();
        return builder.buildJobs(startAtJob);
    }

//    protected List<IHadoopJob> buildJobsOLD() {
//        List<IHadoopJob> holder = new ArrayList<IHadoopJob>();
//        int jobNumber = 0;
//        //noinspection ConstantConditions
//        int startAtJob = getStartAtJob();
//        if (startAtJob <= jobNumber)
//            holder.add(buildJob(getSpectrumPath(), SpectraPeakClustererPass1.class, jobNumber));
//
//        jobNumber++;
//        if (startAtJob <= jobNumber) {
//            String job0Output = getOutputLocation(jobNumber);
//            //noinspection ConstantConditions
//      //      if(START_AT_JOB > 0)    // hard code test input for debugging
//      //             job0Output = "OutputDataTest"; // debug input files
//            IHadoopJob j1 = buildJob(job0Output, SpectraClustererMerger.class, jobNumber);
//            holder.add(j1);
//        }
//
//        jobNumber++;
//        if (startAtJob <= jobNumber)
//        {
//            String job0Output = getOutputLocation(jobNumber);
//            IHadoopJob job = buildJob(job0Output, ClusterConsolidator.class, jobNumber);
//            holder.add(job);
//        }
//        //noinspection UnusedAssignment
//        jobNumber++;
//        return holder;
//    }
//
//    protected IHadoopJob buildJob(String inputFile, Class<? extends IJobRunner> mainClass, int jobNumber, String... addedArgs) {
//        boolean buildJar = isBuildJar();
//        HadoopJob.setJarRequired(buildJar);
//        String outputLocation = getOutputLocation(jobNumber + 1);
//        String spectrumFileName = new File(inputFile).getName();
//        String inputPath = getRemoteBaseDirectory() + "/" + spectrumFileName;
//        String jobDirectory = "jobs";
//        //noinspection UnusedDeclaration
//        int p = getRemoteHostPort();
//        String[] added = buildJobStrings(addedArgs);
//
//
//        //    if (p <= 0)
//        //         throw new IllegalStateException("bad remote host port " + p);
//        IHadoopJob job = HadoopJob.buildJob(
//                mainClass,
//                inputPath,     // data on hdfs
//                jobDirectory,      // jar location
//                outputLocation,
//                added
//        );
//
//        if (getJarFile() != null)
//            job.setJarFile(getJarFile());
//        // reuse the only jar file
//        // job.setJarFile(job.getJarFile());
//
//        incrementPassNumber();
//        return job;
//
//    }


    public void expungeOutputDirectories(IFileSystem accessor) {
        for (int i = getStartAtJob(); i < getNumberStages(); i++) {
            expungeLocalDirectory(accessor, i);

        }
    }

    protected void expungeLocalDirectory(IFileSystem accessor, final int index) {
        String outputLocation = getOutputLocation(index + 1);
        accessor.expunge(outputLocation);
    }


    private static String gParamsFile;
    private static String gPassedBaseDirctory = System.getProperty("user.dir");
    private static boolean gTaskIsLocal = true;

    public static String getParamsFile() {
        return gParamsFile;
    }


    public static boolean isTaskLocal() {
        return gTaskIsLocal;
    }


    public static String getPassedBaseDirctory() {
        if (gPassedBaseDirctory == null)
            return null;
        return gPassedBaseDirctory.replace("\\", "/");
    }

    // params=tandem.params   remoteBaseDirectory=/user/howdah/JXTandem/data/largeSample

    protected static void handleArguments(String[] args) {
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            // print the version and exit
            if ("version".equals(arg)) {
                System.out.println("Spectra Clustering Version = " + CLUSTER_VERSION);
                System.exit(0);
            }

        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            handleArgument(arg);
        }
    }


    protected static void handleArgument(final String pArg) {
        if (pArg.startsWith("params=")) {
            gParamsFile = pArg.substring("params=".length());
            try {
                String paramsFileStr = getParamsFile();
                File paramsFile = new File(paramsFileStr);
                if (!paramsFile.exists())
                    throw new IllegalStateException("params file " + gParamsFile + " does not exist - in the command line you must say params=<paramsFile> and that file MUST exist");
                if (!paramsFile.canRead())
                    throw new IllegalStateException("params file " + gParamsFile + " CANNOT BE READ - in the command line you must say params=<paramsFile> and that file MUST exist");
                File file = paramsFile.getParentFile();
                if (file != null) {
                    String path = file.getPath();
                    String replace = path.replace("\\", "/");
                    gPassedBaseDirctory = RemoteUtilities.getDefaultPath() + "/" + replace;
                }

                InputStream is = new FileInputStream(paramsFile);

                //noinspection ConstantConditions
                if (is == null)
                    throw new IllegalStateException("params file " + gParamsFile + " does not exist - in the command line you must say params=<paramsFile> and that file MUST exist");
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);

            }
            return;
        }
        if (pArg.startsWith("jar=")) {
            setPassedJarFile(pArg.substring("jar=".length()));
            return;
        }
        if (pArg.startsWith("config=")) {
            String cnfigfile = pArg.substring("config=".length());
            handleConfigurationFile(cnfigfile);
            return;
        }
        if (pArg.startsWith("remoteBaseDirectory=")) {
            gPassedBaseDirctory = pArg.substring("remoteBaseDirectory=".length());
        } else {
            throw new IllegalArgumentException("wrong argument provided: " + pArg);
        }


    }

    protected static void handleConfigurationFile(final String pCnfigfile) {
        InputStream describedStream = Util.getDescribedStream(pCnfigfile);

        Properties props = new Properties();
        try {
            props.load(describedStream);
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
        for (String property : props.stringPropertyNames()) {
            String value = props.getProperty(property);
            handleValue(property, value);
        }
    }


    protected static void handleValue(final String pProperty, String pValue) {

        if (pProperty.startsWith("DEFINE_")) {
            String prop = pProperty.substring("DEFINE_".length());
            HadoopUtilities.setHadoopProperty(prop, pValue);
            return;
        }
        if (PARAMS_PROPERTY.equals(pProperty)) {
            gParamsFile = pValue;
            return;
        }
        if (REMOTE_HOST_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            prefs.put("host", pValue);
            return;
        }
        if (REMOTE_PORT_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            prefs.put("port", pValue);
            return;
        }
        if (REMOTE_USER_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            prefs.put("user", pValue);
            return;
        }
        if (REMOTE_JOBTRACKER_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            prefs.put("jobtracker", pValue);
            return;
        }
        if (REMOTE_ENCRYPTED_PASSWORD_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            prefs.put("password", pValue);
            return;
        }
        if (REMOTE_PLAINTEXT_PASSWORD_PROPERTY.equals(pProperty)) {
            Preferences prefs = Preferences.userNodeForPackage(RemoteUtilities.class);
            String encrypted = Encrypt.encryptString(pValue);
            prefs.put("password", encrypted);
            return;
        }
        if (REMOTEDIRECTORY_PROPERTY.equals(pProperty)) {
            String baseDir = pValue;
            String defaultDir = pValue;
            gTaskIsLocal = false;
            if (pValue.startsWith("File://")) {
                gTaskIsLocal = true;
                baseDir = pValue.replace("File://", "");
            }
            if (pValue.endsWith("<LOCAL_DIRECTORY>")) {
                defaultDir = defaultDir.replace("/<LOCAL_DIRECTORY>", "");
                String localName = new File(System.getProperty("user.dir")).getName();
                if (!USER_DIR.equals(getPassedBaseDirctory()))
                    localName = defaultDir + "/" + getPassedBaseDirctory();
                String local = localName;
                baseDir = baseDir.replace("<LOCAL_DIRECTORY>", local);
            }
            String oldBaseDir = RemoteUtilities.getDefaultPath();
            RemoteUtilities.setDefaultPath(defaultDir);

            gPassedBaseDirctory = baseDir;
            return;
        }
        if (MAX_REDUCE_TASKS_PROPERTY.equals(pProperty)) {
            int value = Integer.parseInt(pValue);
            HadoopUtilities.setMaxReduceTasks(value);  // todo fix
            //noinspection ConstantIfStatement
                 //            HadoopUtilities.setMaxReduceTasks(value,HadoopUtilities.getJobSize());
            return;
        }

        if (MAX_SPLIT_SIZE_PROPERTY.equals(pProperty)) {
            int value = Integer.parseInt(pValue);
            HadoopUtilities.setMaxSplitSize(value);
            return;
        }
        if (CLUSTER_SIZE_PROPERTY.equals(pProperty)) {
            int value = Integer.parseInt(pValue);
            HadoopUtilities.setClusterSize(value);
            return;
        }
        if (MAX_CLUSTER_MEMORY_PROPERTY.equals(pProperty)) {
            HadoopUtilities.setProperty(MAX_CLUSTER_MEMORY_PROPERTY, pValue);
            return;
        }
        if (CLUSTER_SIZE_PROPERTY.equals(pProperty)) {
            HadoopUtilities.setProperty(CLUSTER_SIZE_PROPERTY, pValue);
            return;
        }
        if (HADOOP02_HOST.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP02_HOST, pValue);
            return;
        }
        if (HADOOP02_PORT.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP02_PORT, pValue);
            return;
        }
        if (HADOOP02_JOBTRACKER.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP02_JOBTRACKER, pValue);
            return;
        }
        if (HADOOP10_HOST.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP10_HOST, pValue);
            return;
        }
        if (HADOOP10_PORT.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP10_PORT, pValue);
            return;
        }
        if (HADOOP10_JOBTRACKER.equals(pProperty)) {
            HadoopUtilities.setProperty(HADOOP10_JOBTRACKER, pValue);
            return;
        }

        if (DELETE_OUTPUT_DIRECTORIES_PROPERTY.equals(pProperty)) {
            HadoopUtilities.setProperty(DELETE_OUTPUT_DIRECTORIES_PROPERTY, pValue);
            return;
        }
        if (COMPRESS_INTERMEDIATE_FILES_PROPERTY.equals(pProperty)) {
            return;
        }
        throw new UnsupportedOperationException("Property " + pProperty + " with value " + pValue + " Not handled");
    }

    public void copyCreatedFiles(String passedBaseDirctory, String outFileName) {
        AbstractParameterHolder application = getApplication();
        if (application.getBooleanParameter(ClusterLauncher.DO_NOT_COPY_FILES_PROPERTY, false))
            return;
        String[] outputFiles = null;

        String outFile = outFileName;


        String hdfsPath = passedBaseDirctory + "/" + XMLUtilities.asLocalFile(outFile);
        //       String asLocal = XTandemUtilities.asLocalFile("/user/howdah/JXTandem/data/SmallSample/yeast_orfs_all_REV01_short.2011_11_325_10_35_19.t.xml");
        //       String hdfsPathEric = passedBaseDirctory + "/" + "yeast_orfs_all_REV01_short.2011_11_325_10_35_19.t.xml";


        try {
            // case where there is a single outut file
            //noinspection ConstantConditions
            if (outputFiles == null) {
                outFile = copyOutputFile(outFile, hdfsPath);

            } else {
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < outputFiles.length; i++) {
                    outFile = outputFiles[i];
                    hdfsPath = passedBaseDirctory + "/" + XMLUtilities.asLocalFile(outFile);
                    outFile = outFile.replace(".mzXML", "");
                    outFile = copyOutputFile(outFile, hdfsPath);
                }
            }
            //          f = main.readRemoteFile(hdfsPathEric, outFile);

            /**
             * if we are writing out high scoring mgf files then copy them back
             */

            // used for filter on expected value limit_2

            double limit_2 = application.getDoubleParameter(HadoopUtilities.WRITING_MGF_PROPERTY_2, 0);
            double limit = application.getDoubleParameter(HadoopUtilities.WRITING_MGF_PROPERTY, 0);
            if (limit > 0 || limit_2 > 0) {
                String fileName = hdfsPath + ".mgf";
                String outFile2 = outFileName + ".mgf";
                readRemoteFile(fileName, outFile2);
            }


        } catch (IllegalArgumentException e) {
            XMLUtilities.outputLine("Cannot copy remote file " + hdfsPath + " to local file " + outFile +
                    " because " + e.getMessage());
            e.printStackTrace();
            throw e;

        }
    }

    /**
     * really copy a series of files
     *
     * @param outFile
     * @param application
     * @param hdfsPath
     * @return
     */
    private String copyOutputFile(String outFile, String hdfsPath) {
        File f;
        f = readRemoteFile(hdfsPath, outFile);
        if (f != null && f.length() < MAX_DISPLAY_LENGTH) {
            String s = FileUtilities.readInFile(f);
            XMLUtilities.outputLine(s);
        }
        if (outFile.endsWith(".hydra"))
            outFile = outFile.substring(0, outFile.length() - ".hydra".length());

        outFile = outFile.replace(".mzXML", "");
        outFile = outFile.replace(".mzML", "");

        return outFile;
    }


    public static InputStream buildInputStream(String paramsFile) {
        XMLUtilities.outputLine("reading params file " + paramsFile);
        if (paramsFile.startsWith("res://"))
            return Util.getDescribedStream(paramsFile);

        File test = new File(paramsFile);
        if (!test.exists()) {
            XMLUtilities.outputLine("  params file does not exist " + test.getAbsolutePath());
            return null;
        }
        if (!test.canRead()) {
            XMLUtilities.outputLine("  params file cannot be read " + test.getAbsolutePath());
            return null;
        }
        return Util.getDescribedStream(paramsFile);


    }

    /**
     * do all the work in the main but may be run as a different user
     *
     * @param args
     */
    @SuppressWarnings("ConstantConditions")
    public static void workingMain(String[] args) {
        workingMain(args, HadoopDefaults.INSTANCE.getDefaultJobBuilderFactory());
    }

    /**
     * do all the work in the main but may be run as a different user
     *
     * @param args
     */
    @SuppressWarnings("ConstantConditions")
    public static void workingMain(String[] args, IJobBuilderFactory builder) {

        if (builder == null)
            builder = ClusterLauncherJobBuilder.FACTORY;

        ElapsedTimer total = new ElapsedTimer();


        try {
            handleArguments(args);
            String passedBaseDirctory = getPassedBaseDirctory();
            boolean isRemote = !isTaskLocal();
            Configuration cfg = new Configuration();
            if (isRemote) {
                cfg.set("fs.default.name", "hdfs://" + RemoteUtilities.getHost() + ":" + RemoteUtilities.getPort());

            } else {
                cfg.set("fs.default.name", "file:///");    // use local
            }


            String paramsFile = getParamsFile();
            InputStream is = buildInputStream(paramsFile);
            if (is == null) {
                File test = new File(paramsFile);
                XMLUtilities.outputLine("CANNOT RUN BECAUSE CANNOT READ PARAMS FILE " + test.getAbsolutePath());
                return;
            }
            ClusterLauncher main = new ClusterLauncher(is, cfg);
            main.setBuilder(builder.getJobBuilder(main));

            SpectraHadoopMain application = main.getApplication();
            JobSizeEnum jobSize = application.getEnumParameter(HadoopUtilities.JOB_SIZE_PROPERTY, JobSizeEnum.class, JobSizeEnum.Medium);
            HadoopUtilities.setHadoopProperty(HadoopUtilities.JOB_SIZE_PROPERTY, jobSize.toString());

            if (getPassedJarFile() != null) {   // start with a jar file
                main.setJarFile(getPassedJarFile());
                main.setBuildJar(false);
            }
            ElapsedTimer elapsed = main.getElapsed();
            IHadoopController launcher;

            if (isRemote) {
                String host = RemoteUtilities.getHost();
                int port = RemoteUtilities.getPort();
                main.setRemoteHost(host);
                main.setRemoteHostPort(port);
                // make sure directory exists
                IFileSystem access = HDFSAccessor.getFileSystem(host, port);
                main.setAccessor(access);
                String user = RemoteUtilities.getUser(); // "training";  //
                String password = RemoteUtilities.getPassword(); // "training";  //
                RemoteSession rs = new RemoteSession(host, user, password);
                rs.setConnected(true);
                launcher = new RemoteHadoopController(rs);

                main.setRemoteBaseDirectory(passedBaseDirctory);

            } else {
                IFileSystem access = new LocalMachineFileSystem(new File(main.getRemoteBaseDirectory()));
                // locally we have classes - no jar is needed
                main.setAccessor(access);
                main.setBuildJar(false);
                HadoopJob.setJarRequired(false);
                launcher = new LocalHadoopController();
                HadoopUtilities.setMaxReduceTasks(1);  // it rarely helps tp run many reducers locally
            }
            IFileSystem accessor = main.getAccessor();
            accessor.guaranteeDirectory(passedBaseDirctory);

            main.setParamsPath(paramsFile);

            // Temporary fix take oput
//            String hdfsPath1 = HadoopUtilities.getDefaultPath().toString();
//            String[] files = accessor.ls(hdfsPath1);
//            for (int i = 0; i < files.length; i++) {
//                String file = files[i];
//                if (file.startsWith("yeast_orfs_all_REV.20060126.short.task_")) {
//                    Path dx = HadoopUtilities.getRelativePath(file);
//                    accessor.deleteFile(dx.toString());
//                }
//
//            }


            //          main.setPassNumber(1);
            String outFile = main.getOutputFileName();


            //noinspection UnnecessaryLocalVariable
            String defaultBasePath = passedBaseDirctory; //RemoteUtilities.getDefaultPath() + "/JXTandem/JXTandemOutput";
            accessor.guaranteeDirectory(defaultBasePath);
            main.setOutputLocationBase(defaultBasePath + "/OutputData");
            if (isRemote) {
                // make sure directory exists
                // we will kill output directories to guarantee empty
                HDFSUtilities.setOutputDirectoriesPrecleared(true);
                elapsed.showElapsed("Finished Setup");
            }

            main.expungeOutputDirectories(accessor);
            main.runJobs(launcher);

            main.copyCreatedFiles(passedBaseDirctory, outFile);

            main.getElapsed().showElapsed("Capture Output", System.out);

            XMLUtilities.outputLine();
            XMLUtilities.outputLine();
            RunStatistics stats = main.getStatistics();
            XMLUtilities.outputLine(stats.toString());
            total.showElapsed("Total Time", System.out);

            // I think hadoop has launched some threads so we can shut down now
        } catch (Throwable e) {
            e.printStackTrace();
            if (e != e.getCause() && e.getCause() != null) {
                while (e != e.getCause() && e.getCause() != null) {
                    e = e.getCause();
                }
                XMLUtilities.outputLine(e.getMessage());
                StackTraceElement[] stackTrace = e.getStackTrace();
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement se = stackTrace[i];
                    XMLUtilities.outputLine(se.toString());
                }
            }
        }
    }


    // Call with
    // params=tandem.params remoteHost=Glados remoteBaseDirectory=/user/howdah/JXTandem/data/largeSample
    //
    public static void main(final String[] args) throws Exception {
        // reset globals
        SpectraHadoopMain.resetInstance();

        if (args.length == 0) {
            usage();
            return;
        }
        if ("params=".equals(args[1])) {
            usage2();
            return;
        }

        boolean isVersion1 = HadoopMajorVersion.CURRENT_VERSION != HadoopMajorVersion.Version0;


//        HadoopMajorVersion.CURRENT_VERSION = HadoopMajorVersion.Version0; // force version 0.2

        if (!isVersion1) {
            workingMain(args);
        } else {
            //noinspection ConstantConditions
            if (!isVersion1)
                throw new IllegalStateException("This Code will not work under Version 0.2");

//            // by using reflection the class is never loaded when running
//            //noinspection unchecked
//            Class<? extends RunAsUser> cls = (Class<? extends RunAsUser>) Class.forName("uk.ac.ebi.pride.spectracluster.hadoop.RunAsUserUseWithReflection");
//            RunAsUser runner = cls.newInstance();
//            String user = RemoteUtilities.getUser();
//            Object[] passedArgs = {args};
//            runner.runAsUser(user, passedArgs);
        }
    }

}
