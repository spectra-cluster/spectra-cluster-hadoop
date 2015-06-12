package uk.ac.ebi.pride.spectracluster.hadoop.spectrum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import uk.ac.ebi.pride.spectracluster.hadoop.io.MGFInputFormat;
import uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopUtilities;

/**
 * SpectraToClusterJob convert spectra in MGF format into clusters in CGF format
 *
 * The output clusters are stored Hadoop native sequence format
 *
 * @author Rui Wang
 * @version $Id$
 */
public class SpectrumToClusterJob extends Configured implements Tool {
    public static final String WINDOW_SIZE_PROPERTY = "mapper.window_size";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.printf("Usage: %s [generic options] <job name> <job configuration file> <counter file path> <output directory> <input directory> <window size> \n",
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

        Path outputDir = new Path(args[3]);
        FileSystem fileSystem = outputDir.getFileSystem(configuration);
        FileOutputFormat.setOutputPath(job, outputDir);

        // set the window size
        configuration.setFloat(WINDOW_SIZE_PROPERTY, new Float(args[5]));

        // input format
        job.setInputFormatClass(MGFInputFormat.class);
        // output format
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper, reducer and partitioner
        job.setMapperClass(SpectrumToClusterMapper.class);

        // set number of reducer to zero
        job.setNumReduceTasks(0);

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
        int exitcode = ToolRunner.run(new SpectrumToClusterJob(), args);
        System.exit(exitcode);
    }
}
