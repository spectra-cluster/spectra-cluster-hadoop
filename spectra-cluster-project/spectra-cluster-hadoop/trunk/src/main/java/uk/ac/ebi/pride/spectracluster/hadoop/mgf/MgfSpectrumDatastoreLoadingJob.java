package uk.ac.ebi.pride.spectracluster.hadoop.mgf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.systemsbiology.hadoop.ConfiguredJobRunner;
import org.systemsbiology.hadoop.HadoopUtilities;
import org.systemsbiology.hadoop.IJobRunner;
import org.systemsbiology.hadoop.MGFInputFormat;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class MgfSpectrumDatastoreLoadingJob extends ConfiguredJobRunner implements IJobRunner {

    public static final String JOB_NAME = "MGF cluster db loader";

    @Override
    public int runJob(Configuration conf, String[] args) throws Exception {
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, JOB_NAME);
        job.setJarByClass(getClass());

        job.setInputFormatClass(MGFInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputDir = new Path(otherArgs[1]);

        FileOutputFormat.setOutputPath(job, outputDir);
        HadoopUtilities.expunge(outputDir, outputDir.getFileSystem(conf));

        job.setMapperClass(MgfSpectrumDatastoreLoadingMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        return runJob(conf, args);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MgfSpectrumDatastoreLoadingJob(), args);
        System.exit(exitCode);
    }
}
