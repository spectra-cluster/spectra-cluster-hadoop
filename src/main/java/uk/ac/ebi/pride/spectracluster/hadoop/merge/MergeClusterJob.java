package uk.ac.ebi.pride.spectracluster.hadoop.merge;

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
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopUtilities;

/**
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MergeClusterJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.printf("Usage: %s [generic options] <input directory> <output directory> <job name> <job configuration file> <counter file path>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // pre-job configuration
        Configuration configuration = getConf();

        // load custom configurations for the job
        configuration.addResource(args[3]);

        Job job = new Job(configuration, args[2]);
        job.setJarByClass(getClass());

        // configure input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outputDir = new Path(args[1]);
        FileSystem fileSystem = outputDir.getFileSystem(configuration);
        FileOutputFormat.setOutputPath(job, outputDir);

        // input format
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // output format
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper, reducer and partitioner
        job.setMapperClass(MZNarrowBinMapper.class);
        job.setReducerClass(SpectrumMergeReducer.class);
        job.setPartitionerClass(BinPartitioner.class);

        // set output class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean completion = job.waitForCompletion(true);

        if (completion) {
            // output counters for the next job
            String counterFileName = args[4];
            HadoopUtilities.saveCounters(fileSystem, counterFileName, job);
        }

        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MergeClusterJob(), args);
        System.exit(exitCode);
    }
}
