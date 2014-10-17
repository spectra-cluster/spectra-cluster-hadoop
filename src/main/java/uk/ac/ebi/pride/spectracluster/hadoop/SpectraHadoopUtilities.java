package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.utilities.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.systemsbiology.hadoop.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SpectraHadoopUtilities
 * User: Steve
 * Date: 8/13/13
 * static general purpose routines for handling hadoopy things
 */
public class SpectraHadoopUtilities {

    public static final int MIMIMUM_CLUSTER_LENGTH = 5 * "BEGIN IONS\n".length();

    /**
     * convert am int into an mz for easy comparison
     *
     * @param mz input
     * @return MZ_RESOLUTION * mz as int
     */
    public static String mzToKey(double mz) {
        int peak = MZIntensityUtilities.mzToInt(mz);
        //noinspection UnnecessaryLocalVariable
        String str = String.format("%010d", peak);
        return str;
    }

    /**
     * convert am int into an mz for east comparison
     *
     * @param key input
     * @return MZ_RESOLUTION * mz as int
     */
    public static double keyToMZ(String key) {
        //noinspection UnnecessaryLocalVariable
        double ret = Integer.parseInt(key); // (double)MZ_RESOLUTION;
        return ret / MZIntensityUtilities.MZ_RESOLUTION;
    }

    public static Properties readParams(Path params, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs instanceof LocalFileSystem) {
                Path parent = new Path(System.getProperty("user.dir"));
                params = new Path(parent, params.getName());
            }
            FSDataInputStream open = fs.open(params);
            Properties prop = new Properties();
            prop.load(open);
            return prop;
        } catch (IOException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    public static Properties readParamsProperties(Configuration conf, String altName) {
        Properties paramProps = new Properties();
        String params = conf.get(DefaultParameterHolder.PARAMS_KEY);
        params = params.replace("\\", "/");
        if (params == null) {
            conf.set(DefaultParameterHolder.PARAMS_KEY, altName);
        } else {
            paramProps = SpectraHadoopUtilities.readParams(new Path(params), conf);
            String property = paramProps.getProperty(HadoopUtilities.JOB_SIZE_PROPERTY);
            if(property == null)
                property = JobSizeEnum.Medium.toString();
            HadoopUtilities.setProperty(HadoopUtilities.JOB_SIZE_PROPERTY, property);

        }

        return paramProps;
    }


    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param hash    retucer assuming  HadoopUtilities.DEFAULT_NUMBER_REDUCERS is right
     */
    public static void incrementPartitionCounter(Mapper<? extends Writable, Text, Text, Text>.Context context, String prefix, int hash) {
        String counterName = prefix + String.format("%05d", hash).trim();
        context.getCounter("Partition", counterName).increment(1);
    }


    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param hash    retucer assuming  HadoopUtilities.DEFAULT_NUMBER_REDUCERS is right
     */
    public static void incrementPartitionCounter(Reducer<? extends Writable, Text, Text, Text>.Context context, String prefix, int hash) {
        //noinspection ConstantIfStatement
        if (true)
            return;   // not now

        String counterName = prefix + String.format("%05d", hash).trim();
        context.getCounter("Partition", counterName).increment(1);
    }

    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param mzKey   !null key
     */
    @SuppressWarnings("UnusedDeclaration")
    public static void incrementPartitionCounter(Mapper<? extends Writable, Text, Text, Text>.Context context, PeakMZKey mzKey) {
        //noinspection ConstantIfStatement
        if (true)
            return;  // not now
        int hash = mzKey.getPartitionHash() % HadoopUtilities.DEFAULT_TEST_NUMBER_REDUCERS;
        incrementPartitionCounter(context, "Peak", hash);
    }

    /**
     * track how balanced is partitioning
     *
     * @param context !null context
     * @param mzKey   !null key
     */
    public static void incrementPartitionCounter(Mapper<? extends Writable, Text, Text, Text>.Context context, BinMZKey mzKey) {
        //noinspection ConstantIfStatement
        if (true)
            return;  // not now
        int hash = mzKey.getPartitionHash() % HadoopUtilities.DEFAULT_TEST_NUMBER_REDUCERS;
        incrementPartitionCounter(context, "Bin", hash);
    }

    /**
     * build a reader for  a local sequence file
     *
     * @param file !null existing readabl;e non-directory file
     * @param conf !null Configuration
     * @return !null reader
     */
    public static SequenceFile.Reader buildSequenceFileReader(File file, Configuration conf) {
        String fileName = file.getPath();
        Path filePath = new Path(fileName);
        return buildSequenceFileReader(conf, filePath);

    }

    /**
     * build a reader for  a local sequence file
     *
     * @param conf     !null Configuration
     * @param filePath !null existing path
     * @return !null reader
     */
    public static SequenceFile.Reader buildSequenceFileReader(Configuration conf, Path filePath) {
        try {
            FileSystem fs = FileSystem.get(conf);
            return new SequenceFile.Reader(fs, filePath, conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    protected static final String ATTEMPT = "attempt";
    protected static final char SEPARATOR = '_';

    @SuppressWarnings("UnusedDeclaration")
    public static PrintWriter buildReducerWriter(Reducer.Context ctxt, Path basePath, String baseName) {
        try {
            FileSystem fs = basePath.getFileSystem(ctxt.getConfiguration());
            Path path = getAttempPath(ctxt, fs, basePath, baseName);
            final FSDataOutputStream dsOut = fs.create(path);
            System.err.println("Making attempt path " + path);
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            PrintWriter out = new PrintWriter(new OutputStreamWriter(dsOut));
            return out;

        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    @SuppressWarnings("UnusedDeclaration")
    public static Path getAttempPath(final Reducer.Context ctxt, final FileSystem pFs, Path basePath, String baseName) {
        final TaskAttemptID taskAttemptID = ctxt.getTaskAttemptID();
        String str = taskAttemptID.toString();
        //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment,MismatchedReadAndWriteOfArray
        String[] parts = str.split(Character.toString(SEPARATOR));

        // part 4 is attampt number
        String fileName = baseName + parts[4] + ".tmp";
        return new Path(basePath, fileName);
    }


    @SuppressWarnings("UnusedDeclaration")
    public static void renameAttemptFile(Reducer.Context ctxt, Path basePath, String baseName, String outName) {
        try {
            FileSystem fs = basePath.getFileSystem(ctxt.getConfiguration());
            Path pathstartPath = getAttempPath(ctxt, fs, basePath, baseName);
            Path outpath = new Path(basePath, outName);
            System.err.println("Making rename path " + outpath);


            fs.rename(pathstartPath, outpath);

        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    public static String keyToPermanentId(String key) {
        return "SP-" + key;
    }


    public static String permanentIdToKey(String key) {
        return key.substring("SP-".length());
    }

    /**
     * * given a collection of copies of the same cluster make a cluster with spectra in
     * ANY c copy
     *
     * @param key
     * @param values
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("UnusedDeclaration")
    public static ICluster mergeTheSameCluster(String key, Iterable<Text> values,
                                                       Reducer.Context context) throws IOException, InterruptedException {
        String permId = keyToPermanentId(key);
        ICluster merged = new SpectralCluster(permId,Defaults.getDefaultConsensusSpectrumBuilder());

        Map<String, ISpectrum> spectraById = new HashMap<String, ISpectrum>();
        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();
            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);

            String clusterId = cluster.getId();
            if (!clusterId.equals(permId)) {
                throw new IllegalStateException("Merging cl;usters but id " +
                        clusterId + " not the same as key as id " + permId);
            }
            List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
            for (ISpectrum cs : clusteredSpectra) {
                spectraById.put(cs.getId(), cs);
            }
        }

        List<ISpectrum> toAdd = new ArrayList<ISpectrum>(spectraById.values());
        Collections.sort(toAdd);
        merged.addSpectra(toAdd.toArray(new ISpectrum[toAdd.size()]));
        return merged;
    }

    /**
     * given a collection of copies of the same cluster make a cluster with spectra in
     * common to ALL copies
     *
     * @param key
     * @param values
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("UnusedDeclaration")
    public static ICluster deMergeTheSameCluster(String key, Iterable<Text> values,
                                                         Reducer.Context context) throws IOException, InterruptedException {
        String permId = keyToPermanentId(key);
        ICluster merged = new SpectralCluster(permId,Defaults.getDefaultConsensusSpectrumBuilder());

        Map<String, ISpectrum> spectraById = new HashMap<String, ISpectrum>();
        boolean pass1 = true;
        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();
            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);


            String clusterId = cluster.getId();
            if (!clusterId.equals(permId)) {
                throw new IllegalStateException("Merging cl;usters but id " +
                        clusterId + " not the same as key as id " + permId);
            }
            List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
            // first pass add all
            if (pass1) {
                for (ISpectrum cs : clusteredSpectra) {
                    spectraById.put(cs.getId(), cs);
                }
                pass1 = false;
            } else {
                // after that remove all spectra not in common
                Map<String, ISpectrum> localpectraById = new HashMap<String, ISpectrum>();

                for (ISpectrum cs : clusteredSpectra) {
                    localpectraById.put(cs.getId(), cs);
                }

                for (String s : spectraById.keySet()) {
                    if (!localpectraById.containsKey(s))
                        spectraById.remove(s);
                }
                if (spectraById.isEmpty())
                    return merged;
            }
        }

        List<ISpectrum> toAdd = new ArrayList<ISpectrum>(spectraById.values());
        Collections.sort(toAdd);
        merged.addSpectra(toAdd.toArray(new ISpectrum[toAdd.size()]));
        return merged;
    }

    /**
     * make a counter that an engine can handle
     *
     * @param pGroup
     * @param pName
     * @param pContext
     * @return
     */
    @SuppressWarnings("UnusedDeclaration")
    public static IProgressHandler buildProgressCounter(final String pGroup, final String pName, final TaskInputOutputContext pContext) {
        return new CounterProgressHandler(pGroup, pName, pContext);
    }

    private static class CounterProgressHandler implements IProgressHandler {
        private final String group;
        private final String name;
        private final TaskInputOutputContext context;

        private CounterProgressHandler(final String pGroup, final String pName, final TaskInputOutputContext pContext) {
            group = pGroup;
            name = pName;
            context = pContext;
        }

        /**
         * progress is incremented - what this does or means is unclear
         *
         * @param increment amount to increment
         */
        @Override
        public void incrementProgress(final int increment) {
            final Counter counter = context.getCounter(group, name);
            counter.increment(increment);
        }

        /**
         * set progress to 0
         */
        @Override
        public void resetProgress() {
            throw new UnsupportedOperationException("Cannot reset a counter");
        }
    }
}