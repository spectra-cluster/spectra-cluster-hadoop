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

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.printf("Usage: %s [generic options] <job name> <job configuration file> <counter file path> <output directory> <input directory> [multiple cluster result directory]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = getConf();

        // load custom configurations for the job
        configuration.addResource(args[1]);

        Job job = new Job(configuration);
        job.setJobName(args[0]);
        job.setJarByClass(getClass());

        // configure input and output path
        FileInputFormat.addInputPath(job, new Path(args[4]));
        if (args.length > 5) {
            for (int i = 5; i < args.length; i++) {
                FileInputFormat.addInputPath(job, new Path(args[i]));
            }
        }

        Path outputDir = new Path(args[3]);
        FileSystem fileSystem = outputDir.getFileSystem(configuration);
        FileOutputFormat.setOutputPath(job, outputDir);

        // input format
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // output format
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper, reducer and partitioner
        job.setMapperClass(MajorPeakMapper.class);
        job.setReducerClass(MajorPeakReducer.class);
        job.setPartitionerClass(MajorPeakPartitioner.class);

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
