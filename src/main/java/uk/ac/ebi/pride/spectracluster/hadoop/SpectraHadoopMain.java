package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.utilities.*;
import org.apache.hadoop.conf.*;
import org.systemsbiology.hadoop.*;
import org.systemsbiology.xml.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SpectraHadoopMain
 * User: Steve
 * Date: 8/14/13
 */
public class SpectraHadoopMain extends AbstractParameterHolder {
    private static SpectraHadoopMain gInstance;

    /**
     * call to find if we need to build one
     *
     * @return possibly null instance
     */
    public static synchronized SpectraHadoopMain getInstance() {
        return gInstance;
    }

    public static void  resetInstance() {
        gInstance = null;
    }

    /**
     * guarantee this is a singleton
     *
     * @param is
     * @param url
     * @param ctx
     * @return
     */
    public static synchronized SpectraHadoopMain getInstance(InputStream is, @SuppressWarnings("UnusedParameters") Configuration ctx) {
        if (gInstance == null)
            gInstance = new SpectraHadoopMain(is );
        return gInstance;
    }


    private String m_TaskFile;
    private String m_DefaultParameters;
    private String m_OutputPath;
    private String m_OutputResults;
    private final Map<String, String> m_PerformanceParameters = new HashMap<String, String>();


    //    private IScoringAlgorithm m_Scorer;
    private ElapsedTimer m_Elapsed = new ElapsedTimer();

    private final DelegatingFileStreamOpener m_Openers = new DelegatingFileStreamOpener();

    //    public HadoopTandemMain(Configuration ctx )
    //    {
    //        super( );
    //        m_Context = ctx;
    //      }

    //    public HadoopTandemMain(File pTaskFile,Configuration ctx)
    //    {
    //        super(pTaskFile);
    //        m_Context = ctx;
    //      }

    private SpectraHadoopMain(InputStream is  ) {
        m_TaskFile = null;
        //     Protein.resetNextId();
        initOpeners();
        Properties predefined =  HadoopUtilities.getHadoopProperties();
        for (String key : predefined.stringPropertyNames()) {
            setPredefinedParameter(key, predefined.getProperty(key));
        }
        handleInputs(is );

    }

     @SuppressWarnings("UnusedDeclaration")
    public SpectraHadoopMain(final File pTaskFile) {
        m_TaskFile = pTaskFile.getAbsolutePath();
        //      Protein.resetNextId();
        initOpeners();
        Properties predefined =  HadoopUtilities.getHadoopProperties();
        for (String key : predefined.stringPropertyNames()) {
            setPredefinedParameter(key, predefined.getProperty(key));
        }
        try {
            InputStream is = new FileInputStream(m_TaskFile);
            handleInputs(is );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);

        }
        //       if (gInstance != null)
        //           throw new IllegalStateException("Only one XTandemMain allowed");
    }


    private static final List<IStreamOpener> gPreLoadOpeners =
            new ArrayList<IStreamOpener>();

    @SuppressWarnings("UnusedDeclaration")
    public static void addPreLoadOpener(IStreamOpener opener) {
        gPreLoadOpeners.add(opener);
    }

    public static IStreamOpener[] getPreloadOpeners() {
        return gPreLoadOpeners.toArray(new IStreamOpener[gPreLoadOpeners.size()]);
    }


    private static String gRequiredPathPrefix;

    public static String getRequiredPathPrefix() {
        return gRequiredPathPrefix;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void setRequiredPathPrefix(final String pRequiredPathPrefix) {
        gRequiredPathPrefix = pRequiredPathPrefix;
    }


    private void setPredefinedParameter(String key, String value) {
        setParameter(key, value);
      }


    @SuppressWarnings("UnusedDeclaration")
    public void setPerformanceParameter(String key, String value) {
        m_PerformanceParameters.put(key, value);
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

    @SuppressWarnings("UnusedDeclaration")
    public String[] getPerformanceKeys() {
        String[] ret = m_PerformanceParameters.keySet().toArray(new String[m_PerformanceParameters.size()]);
        Arrays.sort(ret);
        return ret;
    }

    /**
     * add new ways to open files
     */
    protected void initOpeners() {
        addOpener(new FileStreamOpener());
        addOpener(new StreamOpeners.ResourceStreamOpener(StreamOpeners.class));
        for (IStreamOpener opener : getPreloadOpeners())
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

    @SuppressWarnings("UnusedDeclaration")
   public ElapsedTimer getElapsed() {
        return m_Elapsed;
    }

    /**
     * parse the initial file and get run parameters
     *
     * @param is
     */
    public void handleInputs(final InputStream is ) {
        Properties notes = new Properties();
        try {
            notes.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (String key : notes.stringPropertyNames()) {
            setParameter(key, notes.getProperty(key));
            System.err.println(key + " = " + notes.get(key));
        }
        m_DefaultParameters = notes.getProperty(
                "list path, default parameters"); //, "default_input.xml");
        m_OutputPath = notes.getProperty("output, path"); //, "output.xml");
        // little hack to separate real tandem and hydra results
        if (m_OutputPath != null)
            m_OutputPath = m_OutputPath.replace(".tandem.xml", ".hydra.xml");

        m_OutputResults = notes.getProperty("output, results");

        String requiredPrefix = getRequiredPathPrefix();
        if (requiredPrefix != null) {
            if (m_DefaultParameters != null && !m_DefaultParameters.startsWith(requiredPrefix))
                m_DefaultParameters = requiredPrefix + m_DefaultParameters;
            if (m_OutputPath != null && !m_OutputPath.startsWith(requiredPrefix))
                m_OutputPath = requiredPrefix + m_OutputPath;
        }

        try {
            readDefaultParameters(notes);
        } catch (Exception e) {
            // forgive
            System.err.println("Cannot find file " + m_DefaultParameters);
        }


    }


     @SuppressWarnings("UnusedDeclaration")
    protected static File getInputFile(Map<String, String> notes, String key) {
        File ret = new File(notes.get(key));
        if (!ret.exists() || !ret.canRead())
            throw new IllegalArgumentException("cannot access file " + ret.getName());
        return ret;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected static File getOutputFile(Map<String, String> notes, String key) {
        String athname = notes.get(key);
        File ret = new File(athname);
        File parentFile = ret.getParentFile();
        if ((parentFile != null && (!parentFile.exists() || parentFile.canWrite())))
            throw new IllegalArgumentException("cannot access file " + ret.getName());
        if (ret.exists() && !ret.canWrite())
            throw new IllegalArgumentException("cannot rewrite file file " + ret.getName());

        return ret;
    }


    @SuppressWarnings("UnusedDeclaration")
    public String getTaskFile() {
        return m_TaskFile;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getDefaultParameters() {
        return m_DefaultParameters;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getOutputPath() {
        return m_OutputPath;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getOutputResults() {
        return m_OutputResults;
    }


    /**
     * read the parameters dscribed in the bioml file
     * listed in "list path, default parameters"
     * These look like
     * <note>spectrum parameters</note>
     * <note type="input" label="spectrum, fragment monoisotopic mass error">0.4</note>
     * <note type="input" label="spectrum, parent monoisotopic mass error plus">100</note>
     * <note type="input" label="spectrum, parent monoisotopic mass error minus">100</note>
     * <note type="input" label="spectrum, parent monoisotopic mass isotope error">yes</note>
     */
    protected void readDefaultParameters(Properties inputParameters) {
        Map<String, String> parametersMap = getParametersMap();
        if (m_DefaultParameters != null) {
            String paramName;
            InputStream is;
            if (m_DefaultParameters.startsWith("res://")) {
                is = Util.getDescribedStream(m_DefaultParameters);
                paramName = m_DefaultParameters;
            } else {
                File f = new File(m_DefaultParameters);
                if (f.exists() && f.isFile() && f.canRead()) {
                    try {
                        is = new FileInputStream(f);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);

                    }
                    paramName = f.getName();
                } else {
                    paramName = XMLUtilities.asLocalFile(m_DefaultParameters);
                    is = open(paramName);
                }
            }
            if (is == null) {
                throw new IllegalArgumentException("the default input file designated by \"list path, default parameters\" " + m_DefaultParameters + "  does not exist"); // ToDo change

            } else {
                Map<String, String> map = HadoopUtilities.readNotes(is, paramName);
                for (String key : map.keySet()) {
                    if (!parametersMap.containsKey(key)) {
                        String value = map.get(key);
                        parametersMap.put(key, value);
                    }
                }
            }
        }
        // parameters in the input file override parameters in the default file
        for(String key : inputParameters.stringPropertyNames())    {
            parametersMap.put(key,inputParameters.getProperty(key)) ;
        }
     }

    protected static void usage() {
        System.out.println("Usage <input file or directory> <output directory>");
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }


        String[] pass1Args = {args[0], "output1"};
        SpectraPeakClustererPass1.main(pass1Args);

        String[] pass2Args = {"output1", "output2"};
        SpectraClustererMerger.main(pass2Args);

        String[] pass3Args = {"output2", "output3"};
        ClusterConsolidator.main(pass3Args);
    }

}
