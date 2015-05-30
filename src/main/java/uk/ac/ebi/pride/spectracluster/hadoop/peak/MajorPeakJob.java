package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopUtilities;

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

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.printf("Usage: %s [generic options] <job name> <job configuration file> <counter file path> <cluster similarity threshold> <current round> <output directory> <input directory> [multiple cluster result directory]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = getConf();

        // load custom configurations for the job
        configuration.addResource(args[1]);

        // similarity threshold
        configuration.setFloat(ConfigurableProperties.SIMILARITY_THRESHOLD_PROPERTY, new Float(args[3]));
        configuration.setInt(CURRENT_CLUSTERING_ROUND, new Integer(args[4]));

        Job job = new Job(configuration);
        job.setJobName(args[0]);
        job.setJarByClass(getClass());

        // configure input and output path
        FileInputFormat.addInputPath(job, new Path(args[6]));
        if (args.length > 7) {
            for (int i = 7; i < args.length; i++) {
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
        }

        Path outputDir = new Path(args[5]);
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


    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new MajorPeakJob(), args);
        System.exit(exitcode);
    }
}
