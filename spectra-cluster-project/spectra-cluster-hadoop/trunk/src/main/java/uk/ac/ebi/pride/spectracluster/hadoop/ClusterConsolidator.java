package uk.ac.ebi.pride.spectracluster.hadoop;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.systemsbiology.hadoop.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;


/**
 * uk.ac.ebi.pride.spectracluster.hadoop.ClusterConsolidator
 * This sends ALL output to a single reducer to make a CGF file -
 * primarily for use in early stage debugging
 */
public class ClusterConsolidator extends ConfiguredJobRunner implements IJobRunner {

    public static final int BIG_CLUSTER_SIZE = 10;
    public static final int NUMBER_REDUCE_JOBS = 120;

    public static final String CONSOLIDATOR_PATH_PROPERTY = "uk.ebi.ac.consolidatedPath";

    public static class MZKeyMapper extends AbstractParameterizedMapper<Text> {


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String label = key.toString();
            String text = value.toString();
            if (label == null || text == null)
                return;
            if (label.length() == 0 || text.length() == 0)
                return;

            LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
            ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);
            // should be only one
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < clusters.length; i++) {
                ICluster cluster = clusters[i];
                MZKey mzkey = new MZKey(cluster.getPrecursorMz());
                final String keyStr = mzkey.toString();
                writeKeyValue(keyStr, text, context);
            }
        }

    }

    protected static void usage() {
        System.out.println("Usage <input file or directory> <output directory>");
    }


    public int runJob(Configuration conf, final String[] args) throws Exception {
        try {
            if (args.length < 2)
                throw new IllegalStateException("needs a file name and an output directory");
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            // GenericOptionsParser stops after the first non-argument
            otherArgs = HadoopUtilities.handleGenericInputs(conf, otherArgs);


//        if (otherArgs.length != 2) {
//            System.err.println("Usage: wordcount <in> <out>");
//            System.exit(2);
//        }
            Job job = new Job(conf, "Cluster Consolidator");
            setJob(job);

            conf = job.getConfiguration(); // NOTE JOB Copies the configuraton


            // make default settings
            HadoopUtilities.setDefaultConfigurationArguments(conf);

            // sincs reducers are writing to hdfs we do NOT want speculative execution
            // conf.set("mapred.reduce.tasks.speculative.execution", "false");


            Properties paramProps = SpectraHadoopUtilities.readParamsProperties(conf, otherArgs[0]);
            job.setJarByClass(ClusterConsolidator.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);

            // there is no output
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(MZKeyMapper.class);
            job.setReducerClass(FileWriteReducer.class);
            // partition by MZ as int
            job.setPartitionerClass(MZPartitioner.class);

            // never do speculative execution
            conf.set("mapreduce.reduce.speculative", "false");


            // We always do this
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            // Do not set reduce tasks - ue whatever cores are available
            // this does not work just set a number for now
            // HadoopUtilities.setRecommendedMaxReducers(job);
            job.setNumReduceTasks(NUMBER_REDUCE_JOBS);  //  this should scale well enough


            Path outPath = null;
            if (otherArgs.length > 1) {
                String otherArg = otherArgs[0];
                Path inputPath = HadoopUtilities.setInputPath(job, otherArg);
                System.err.println("Input path mass finder " + otherArg);

                Path parentPath = inputPath.getParent();
                outPath = new Path(parentPath, HadoopDefaults.getOutputPath());

                FileSystem fileSystem = outPath.getFileSystem(conf);
            //    fileSystem.delete(outPath,true); // drop prior contents
                fileSystem.mkdirs(outPath);
                String outPathName = outPath.toString();
                System.out.println(outPathName);
                conf.set(CONSOLIDATOR_PATH_PROPERTY, outPathName);
            }

            // you must pass the output directory as the last argument
            String athString = otherArgs[otherArgs.length - 1];
            //           File out = new File(athString);
//        if (out.exists()) {
//            FileUtilities.expungeDirectory(out);
//            out.delete();
//        }

            Path outputDir = new Path(athString);

            FileSystem fileSystem = outputDir.getFileSystem(conf);
            fileSystem.delete(outputDir,true); // drop prior contents
         //   HadoopUtilities.expunge(outputDir, fileSystem);    // make sure thia does not exist
            FileOutputFormat.setOutputPath(job, outputDir);
            System.err.println("Output path mass finder " + outputDir);


            boolean ans = job.waitForCompletion(true);
            int ret = ans ? 0 : 1;
            if (ans) {
                String fileName = HadoopUtilities.buildCounterFileName(this, conf);
                HadoopUtilities.saveCounters(fileSystem, fileName, job);

                HadoopUtilities.deleteTmpFiles(outPath, conf);
            } else
                throw new IllegalStateException("Job Failed");

            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        if (conf == null)
            conf = HDFSAccessor.getSharedConfiguration();
        return runJob(conf, args);
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        ToolRunner.run(new ClusterConsolidator(), args);
    }
}