package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.io.DotClusterClusterAppender;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Reducer to write clusters to the final .clustering file output format
 *
 * @author Rui Wang
 * @version $Id$
 */
public class FileWriteReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    private static final String CLUSTERING_FILE_EXTENSION = "clustering";

    private String clusteringFilePrefix;
    private MZKey currentKey;
    // TODO: this includes the spectra in the .clustering files...
    private DotClusterClusterAppender clusterAppender = DotClusterClusterAppender.PEAK_INSTANCE;
    private PrintWriter currentFileWriter;
    private final Set<String> currentClusteredSpectraIds = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String prefix = context.getConfiguration().get("clustering.file.prefix", "");
        setClusteringFilePrefix(prefix);

        String includeSpectraString = context.getConfiguration().get("clustering.file.include.spectra", "");
        boolean includeSpectra = (includeSpectraString == null) ? false : Boolean.parseBoolean(includeSpectraString);
        setClusterAppender(includeSpectra? DotClusterClusterAppender.PEAK_INSTANCE : DotClusterClusterAppender.INSTANCE);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        MZKey mzKey = new MZKey(key.toString());

        MZKey currKey = getCurrentKey();
        if (currKey == null || currKey.getAsInt() != mzKey.getAsInt()) {
            updateFileAppender(context, mzKey);
            setCurrentKey(mzKey);
            currentClusteredSpectraIds.clear();
        }

        for (Text value : values) {
            // parse cluster
            String content = value.toString();
            ICluster cluster = IOUtilities.parseClusterFromCGFString(content);

            String combinedSpectraId = cluster.getSpectralId();
            if (!currentClusteredSpectraIds.contains(combinedSpectraId)) {
                // record the combined spectra id
                currentClusteredSpectraIds.add(combinedSpectraId);

                // write cluster
                getClusterAppender().appendCluster(getCurrentFileWriter(), cluster);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        updateFileAppender(context, null);
        currentClusteredSpectraIds.clear();
        super.cleanup(context);
    }

    private void updateFileAppender(Context context, MZKey mzKey) {
        if (getCurrentFileWriter() != null) {
            getCurrentFileWriter().flush();
            getCurrentFileWriter().close();
            setCurrentFileWriter(null);
        }

        // set the new key
        if (mzKey == null)
            return;

        // create a new print writer to write to a different file
        try {
            Configuration configuration = context.getConfiguration();
            String outputDir = configuration.get("mapred.output.dir");
            Path parentPath = new Path(outputDir);
            String outputFileName = clusteringFilePrefix + String.format("%04d", mzKey.getAsInt()) + "." + CLUSTERING_FILE_EXTENSION;
            Path outputFilePath = new Path(parentPath, outputFileName);
            FileSystem fileSystem = parentPath.getFileSystem(configuration);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(outputFilePath);
            setCurrentFileWriter(new PrintWriter(new OutputStreamWriter(fsDataOutputStream)));

            // append header
            getClusterAppender().appendDotClusterHeader(getCurrentFileWriter(), outputFilePath.getName());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create a new clustering file", ex);
        }
    }

    public MZKey getCurrentKey() {
        return currentKey;
    }

    public void setCurrentKey(MZKey currentKey) {
        this.currentKey = currentKey;
    }

    public DotClusterClusterAppender getClusterAppender() {
        return clusterAppender;
    }

    public void setClusterAppender(DotClusterClusterAppender clusterAppender) {
        this.clusterAppender = clusterAppender;
    }

    public PrintWriter getCurrentFileWriter() {
        return currentFileWriter;
    }

    public void setCurrentFileWriter(PrintWriter currentFileWriter) {
        this.currentFileWriter = currentFileWriter;
    }

    public String getClusteringFilePrefix() {
        return clusteringFilePrefix;
    }

    public void setClusteringFilePrefix(String clusteringFilePrefix) {
        this.clusteringFilePrefix = clusteringFilePrefix;
    }
}
