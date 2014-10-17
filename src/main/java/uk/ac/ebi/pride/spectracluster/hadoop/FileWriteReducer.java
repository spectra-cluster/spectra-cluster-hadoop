package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * retucer to w
 */
public class FileWriteReducer extends Reducer<Text, Text, NullWritable, Text> {


    @SuppressWarnings("UnusedDeclaration")
    public static String getFileNameString(MZKey key) {
        return "BinFile" + String.format("%04d", key.getAsInt());
    }

    @SuppressWarnings("UnusedDeclaration")
    public static String getBigClusterFileNameString(MZKey key) {
        return "BigClusterFile" + String.format("%04d", key.getAsInt());
    }


    //    private PrintWriter outWriter;
//    private PrintWriter bigOutWriter;
//    private boolean currentWriterWritten;
//    private boolean bigCurrentWriterWritten;
    private Path basePath;
    private int numberWritten;
    private MZKey currentKey;
    private final List<ClusterCreateListener> m_ClusterCreateListeners = new ArrayList<ClusterCreateListener>();


    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        String pathName = configuration.get(ClusterConsolidator.CONSOLIDATOR_PATH_PROPERTY);
        basePath = new Path(pathName);
        System.err.println("Base Path Name " + pathName);
    }


    /**
     * add a change listener
     * final to make sure this is not duplicated at multiple levels
     *
     * @param added non-null change listener
     */
    protected final void addClusterCreateListener(ClusterCreateListener added) {
        if (!m_ClusterCreateListeners.contains(added))
            m_ClusterCreateListeners.add(added);
    }

    /**
     * remove a change listener
     *
     */
    protected final void clearClusterCreateListeners() {
        for (ClusterCreateListener listener : m_ClusterCreateListeners) {
            listener.onClusterCreateFinished();

        }
        m_ClusterCreateListeners.clear();
    }

    public Path getBasePath() {
        return basePath;
    }

    @SuppressWarnings("UnusedDeclaration")
    public int getNumberWritten() {
        return numberWritten;
    }

    /**
     * here is where file formats are selected
     * @param context
     */
    protected void buildWritersForKey(final Context context) {
        MZKey key = getCurrentKey();
        if (key == null)
            return;
        Path base = getBasePath();
        IClusterAppender appender;

        ClusterSizeFilter bigOnly = new ClusterSizeFilter(ClusterConsolidator.BIG_CLUSTER_SIZE); // only accept big custers

        // full cgf
//        appender = CGFClusterAppender.INSTANCE;
//        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.CGF_INSTANCE));

        // only big cgf
//        appender = new FilteredClusterAppender(appender, bigOnly);
//        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.BIG_CGF_INSTANCE));

        // full .cluster
        appender = new DotClusterClusterAppender();
        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.CLUSTERING_INSTANCE));

//        // only big .cluster
//        appender = new FilteredClusterAppender(appender, bigOnly);   // not only big ones
//        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.BIG_CLUSTERING_INSTANCE));

             // full .cluster
//        appender = SPTextClusterAppender.INSTANCE;
//        appender = new FilteredClusterAppender(appender, bigOnly);   // not only big ones
//        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.SPTEXT_CLUSTERING_INSTANCE));

//        // only big .cluster
//        appender = MSFClusterAppender.INSTANCE;
//        appender = new FilteredClusterAppender(appender, bigOnly);   // not only big ones
//        addClusterCreateListener(new DotClusterPathListener(base, key, context, appender, PathFromMZGenerator.MSP_CLUSTERING_INSTANCE));

    }


    /**
     * notify any state change listeners - probably should
     * be protected but is in the interface to form an event cluster
     *
     */
    public void notifyClusterCreateListeners(ICluster cluster) {
        for (ClusterCreateListener listener : m_ClusterCreateListeners) {
            listener.onClusterCreate(cluster);
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public MZKey getCurrentKey() {
        return currentKey;
    }

//    public void setCurrentWriterWritten(boolean currentWriterWritten) {
//        if (this.currentWriterWritten == currentWriterWritten)
//            return;
//        this.currentWriterWritten = currentWriterWritten;
//    }
//
//    public PrintWriter getWorkingWriter(final Context context) {
//        PrintWriter ret = internalGetOutWriter();
//        if (ret != null)
//            return ret;
//        String baseName = getFileNameString(getCurrentKey());
//        PrintWriter outWriter2 = SpectraHadoopUtilities.buildReducerWriter(context, basePath, baseName);
//        setOutWriter(outWriter2);
//
//        return internalGetOutWriter();
//    }
//
//    public PrintWriter internalGetOutWriter() {
//        return outWriter;
//    }
//
//    public void setOutWriter(PrintWriter ow) {
//        outWriter = ow;
//        setCurrentWriterWritten(false);
//    }
//
//    public boolean isCurrentWriterWritten() {
//        return currentWriterWritten;
//    }
//
//
//    public PrintWriter getWorkingBigWriter(final Context context) {
//        PrintWriter ret = internalGetBigOutWriter();
//        if (ret != null)
//            return ret;
//        String baseName = getBigClusterFileNameString(getCurrentKey());
//        PrintWriter outWriter2 = SpectraHadoopUtilities.buildReducerWriter(context, basePath, baseName);
//        setBigOutWriter(outWriter2);
//
//        return internalGetBigOutWriter();
//    }
//
//    public PrintWriter internalGetBigOutWriter() {
//        return bigOutWriter;
//    }
//
//    public void setBigOutWriter(final PrintWriter pBigOutWriter) {
//        bigOutWriter = pBigOutWriter;
//        setBigCurrentWriterWritten(false);
//    }
//
//
//    public boolean isBigCurrentWriterWritten() {
//        return bigCurrentWriterWritten;
//    }
//
//    public void setBigCurrentWriterWritten(final boolean pBigCurrentWriterWritten) {
//        if (bigCurrentWriterWritten == pBigCurrentWriterWritten)
//            return;
//        bigCurrentWriterWritten = pBigCurrentWriterWritten;
//    }

    public void setCurrentKey(MZKey key, final Context context) {

        // let the writeers to the dirty work
        clearClusterCreateListeners();
//        final PrintWriter outWriter1 = internalGetOutWriter();
//        if (outWriter1 != null) {
//            outWriter1.close();
//            if (isCurrentWriterWritten()) {
//                String baseName = getFileNameString(currentKey);
//                // this is a concurrency error
//                SpectraHadoopUtilities.renameAttemptFile(context, basePath, baseName, getFileNameString(currentKey) + ".cgf");
//            }
//            setOutWriter(null);
//        }
//        final PrintWriter bigOutWriter1 = internalGetBigOutWriter();
//        if (bigOutWriter1 != null) {
//            bigOutWriter1.close();
//            if (isBigCurrentWriterWritten()) {
//                String baseName = getBigClusterFileNameString(currentKey);
//                SpectraHadoopUtilities.renameAttemptFile(context, basePath, baseName, getBigClusterFileNameString(currentKey) + ".cgf");
//            }
//            setBigOutWriter(null);
//        }
        currentKey = key;
        if (key == null)
            return;
        buildWritersForKey(context);
        numberWritten = 0;
    }


    /**
     * write the values to a real file -
     * NOT useful in a cluster but very helpful in debugging
     *
     * @param key
     * @param values
     * @param context
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

        MZKey realkey = new MZKey(key.toString());
        MZKey currentKey1 = getCurrentKey();
        int currentKeyInt = 0;
        if (currentKey1 != null)
            currentKeyInt = currentKey1.getAsInt();
        int keyInt = realkey.getAsInt();
        if (currentKeyInt != keyInt) {
            setCurrentKey(realkey, context);
        }
        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();
//            final PrintWriter outputWriter1 = getWorkingWriter(context);    // get creating as needed
//            outputWriter1.println(valStr);
//            setCurrentWriterWritten(true);
//            if (numberWritten++ % 100 == 0)
//                System.err.println("Wrote " + numberWritten);
//
            // now write the big clusters
            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);

            notifyClusterCreateListeners(cluster);
//
//            // write the big clusters in a different file
//            final int clusteredSpectraCount = cluster.getClusteredSpectraCount();
//            if (clusteredSpectraCount >= ClusterConsolidator.BIG_CLUSTER_SIZE) {
//                final PrintWriter bigOutWriter1 = getWorkingBigWriter(context);    // get creating as needed
//                bigOutWriter1.println(valStr);
//                setBigCurrentWriterWritten(true);
//            }

        }

    }


    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        setCurrentKey(null, context);
    }


}
