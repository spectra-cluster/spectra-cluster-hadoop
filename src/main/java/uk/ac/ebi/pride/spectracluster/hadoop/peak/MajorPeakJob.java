package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import uk.ac.ebi.pride.spectracluster.hadoop.merge.BinPartitioner;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopClusterProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopUtilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * MajorPeakJob performs incremental clustering on clusters that share the same peaks with high intensities
 *
 * NOTE: this job can take in multiple input directories, this is mainly used for
 * loading previous clustering results
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MajorPeakJob extends Configured implements Tool {
    public static final String CURRENT_CLUSTERING_ROUND = "clustering.current.round";
    public static final String CURRENT_BINNER_WINDOW_SIZE = "mapper.window.size";
    public static final int MAX_SPECTRA_PER_BIN = 50000;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 9) {
            //                                                  0                 1                    2                          3                       4                      5             6                 7                    9
            System.err.printf("Usage: %s [generic options] <job name> <job configuration file> <counter file path> <cluster similarity threshold> <mapper window size> <current round> <output directory> <input directory> <binning counter file> [multiple cluster result directory]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = getConf();

        // load custom configurations for the job
        configuration.addResource(args[1]);

        // load the binning counter file
        String binningCounterFilePath = args[8];
        configuration = setBinMappings(configuration, binningCounterFilePath);

        // similarity threshold
        configuration.setFloat(ConfigurableProperties.SIMILARITY_THRESHOLD_PROPERTY, new Float(args[3]));
        configuration.setFloat (CURRENT_BINNER_WINDOW_SIZE, new Float(args[4]));
        configuration.setInt(CURRENT_CLUSTERING_ROUND, new Integer(args[5]));

        Job job = new Job(configuration);
        job.setJobName(args[0]);
        job.setJarByClass(getClass());

        // configure input and output path
        FileInputFormat.addInputPath(job, new Path(args[7]));
        if (args.length > 9) {
            for (int i = 9; i < args.length; i++) {
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
        }

        Path outputDir = new Path(args[6]);
        FileSystem fileSystem = outputDir.getFileSystem(configuration);
        FileOutputFormat.setOutputPath(job, outputDir);

        // input format
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // output format
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper, reducer and partitioner
        job.setMapperClass(MajorPeakMapper.class);
        job.setReducerClass(MajorPeakReducer.class);
        job.setPartitionerClass(BinPartitioner.class);

        // set output class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean completion = job.waitForCompletion(true);

        if (completion) {
            // output counters for the next job
            String counterFileName = args[2];
            HadoopUtilities.saveCounters(fileSystem, counterFileName, job);
        }

        return completion ? 0 : 1;
    }

    private Configuration setBinMappings(Configuration configuration, String binningCounterFilePath) throws Exception {
        // TODO: this was disabled since we may only have 120 bins...
        if (true) return configuration;

        // open the file
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream inputStream = fs.open(new Path(binningCounterFilePath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        int minBinIndex = Integer.MAX_VALUE;
        int maxBinIndex = 0;

        Map<Integer, Integer> binSizePerIndex = new HashMap<Integer, Integer>();

        while ((line = reader.readLine()) != null) {
            if (!line.startsWith(HadoopClusterProperties.BIN_PREFIX)) {
                continue;
            }

            String binSetting = line.substring(HadoopClusterProperties.BIN_PREFIX.length());

            int index = binSetting.indexOf('=');

            if (index < 0) {
                throw new Exception("Malformatted bin size definition: " + line);
            }

            int binIndex = new Integer(binSetting.substring(0, index));
            int binSize = new Integer(binSetting.substring(index + 1));

            binSizePerIndex.put(binIndex, binSize);

            if (binIndex > maxBinIndex) {
                maxBinIndex = binIndex;
            }
            if (binIndex < minBinIndex) {
                minBinIndex = binIndex;
            }
        }

        if (binSizePerIndex.size() < 1)
            throw new Exception("Failed to load bin sizes");

        // merge adjacent bins if they are too small
        int currentBinIndex = -1;
        int currentBinSize = MAX_SPECTRA_PER_BIN; // this forces the currentBinIndex to be adapted to the first bin

        for (int binIndex = minBinIndex; binIndex <= maxBinIndex; binIndex++) {
            int size;

            // simply ignore bin indexes that don't exist - this should not happen
            if (!binSizePerIndex.containsKey(binIndex)) {
                size = 0;
            }
            else {
                size = binSizePerIndex.get(binIndex);
            }

            // move to the new bin if necessary
            if (currentBinSize + size > MAX_SPECTRA_PER_BIN) {
                // update the current bin to this bin
                currentBinIndex = binIndex;
                currentBinSize = size;
            }
            else {
                currentBinSize += size; // simply add this bin to the current one
            }

            // save the mapping in the configuration
            configuration.setInt(getMappingStringForSpectrumBin(binIndex), currentBinIndex);
        }

        return configuration;
    }


    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new MajorPeakJob(), args);
        System.exit(exitcode);
    }

    public static String getMappingStringForSpectrumBin(int bin) {
        return String.format("mapping_%d", bin);
    }
}
