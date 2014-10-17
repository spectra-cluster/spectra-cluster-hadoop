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
import uk.ac.ebi.pride.spectracluster.engine.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;


/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SameClustererMerger
 * This merges clusters that are the same as identified by the same ID
 * it is used after clusters are sent to multiple reducers
 */
public class SameClustererMerger extends ConfiguredJobRunner implements IJobRunner {


    /**
     * mapper sends out clusters with the same id (highest quality spectrum)
     * clusters with the same id are merged
     */
    public static class ClusterIdMapper extends AbstractParameterizedMapper<Text> {


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
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < clusters.length; i++) {
                ICluster cluster = clusters[i];

                // cluster becomes cgf
                StringBuilder sb = new StringBuilder();
                final DotClusterClusterAppender clusterAppender = new DotClusterClusterAppender();
                clusterAppender.appendCluster(sb, cluster);
                writeKeyValue(cluster.getId(), sb.toString(), context);
            }
        }

    }

    /**
     * merges clusters with the same id (highest quality spectrum)
     */
    public static class ClusterIdReducer extends AbstractParameterizedReducer {

        private IncrementalClusteringEngineFactory factory = new IncrementalClusteringEngineFactory();
        private IIncrementalClusteringEngine engine;


        public IIncrementalClusteringEngine getEngine() {
            if (engine == null) {
                engine = factory.getIncrementalClusteringEngine((float)HadoopDefaults.getSameClusterMergeMZWindowSize());
            }
            return engine;
        }

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            ConfigurableProperties.configureAnalysisParameters(getApplication());
         }



        /**
         * this reducer get all clusters with the same id, usually the highest quality spectrum and merges them
         * use it when the samer cluster is sent to multiple reducers
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduceNormal(Text key, Iterable<Text> values,
                                 Context context) throws IOException, InterruptedException {

            String id = key.toString();

            int numberProcessed = 0;

            ICluster ret = new SpectralCluster(id,Defaults.getDefaultConsensusSpectrumBuilder());

            //noinspection LoopStatementThatDoesntLoop
            for (Text val : values) {
                String valStr = val.toString();

                LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
                final ICluster cluster = ParserUtilities.readSpectralCluster(rdr, null);

                if (cluster == null) {  // todo why might this happen
                    continue;
                }
                List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
                for (ISpectrum spc : clusteredSpectra) {
                    ret.addSpectra(spc);
                    numberProcessed++;
                }
                numberProcessed++;
            }
            // track how many
            context.getCounter("Performance", "MergedSameCluster").increment(numberProcessed);

            if (ret.getConsensusSpectrum().getPeaksCount() > 0)
                writeCluster(context, ret);
        }

        /**
         * write one cluster and key
         *
         * @param context !null context
         * @param cluster !null cluster
         */
        protected void writeCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
            final List<ICluster> allClusters = ClusterUtilities.findNoneFittingSpectra(cluster, engine.getSimilarityChecker(),Defaults.getRetainThreshold());
            if (!allClusters.isEmpty()) {
                for (ICluster removedCluster : allClusters) {

                    // drop all spectra
                    final List<ISpectrum> clusteredSpectra = removedCluster.getClusteredSpectra();
                    ISpectrum[] allRemoved = clusteredSpectra.toArray(new ISpectrum[clusteredSpectra.size()]);
                    cluster.removeSpectra(allRemoved);

                    // and write as stand alone
                    writeOneCluster(context, removedCluster);
                }

            }
            // now write the original
            if (cluster.getClusteredSpectraCount() > 0)
                writeOneCluster(context, cluster);     // nothing removed
        }


        protected void writeOneCluster(final Context context, final ICluster cluster) throws IOException, InterruptedException {
            if (cluster.getClusteredSpectraCount() == 0)
                return; // empty dont bother

            float precursorMz = cluster.getPrecursorMz();
            MZKey key = new MZKey(precursorMz);
            String keyStr = key.toString();

            StringBuilder sb = new StringBuilder();
            final CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
            clusterAppender.appendCluster(sb, cluster);
            String string = sb.toString();

            if (string.length() > SpectraHadoopUtilities.MIMIMUM_CLUSTER_LENGTH) {
                writeKeyValue(keyStr, string, context);

            }
        }


        /**
         * Called once at the end of the task.
         */
        @Override
        protected void cleanup(final Context context) throws IOException, InterruptedException {
            //    writeParseParameters(context);
            super.cleanup(context);

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
            Job job = new Job(conf, "Same Cluster Merger");
            setJob(job);

            conf = job.getConfiguration(); // NOTE JOB Copies the configuraton

            // make default settings
            HadoopUtilities.setDefaultConfigurationArguments(conf);

            // since reducers are writing to hdfs we do NOT want speculative execution
            // conf.set("mapred.reduce.tasks.speculative.execution", "false");



            @SuppressWarnings("UnusedDeclaration")
            Properties paramProps = SpectraHadoopUtilities.readParamsProperties(conf, otherArgs[0]);

            job.setJarByClass(SameClustererMerger.class);



            job.setInputFormatClass(SequenceFileInputFormat.class);

            // sequence files are faster but harder to debug
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setMapperClass(ClusterIdMapper.class);
            job.setReducerClass(ClusterIdReducer.class);


            // We always do this
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            // Do not set reduce tasks - ue whatever cores are available
            // this does not work just set a number for now
            HadoopUtilities.setRecommendedMaxReducers(job);


            if (otherArgs.length > 1) {
                String otherArg = otherArgs[0];
                HadoopUtilities.setInputPath(job, otherArg);
                System.err.println("Input path mass finder " + otherArg);
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
            HadoopUtilities.expunge(outputDir, fileSystem);    // make sure thia does not exist
            FileOutputFormat.setOutputPath(job, outputDir);
            System.err.println("Output path mass finder " + outputDir);


            boolean ans = job.waitForCompletion(true);
            int ret = ans ? 0 : 1;
            if (ans)
                HadoopUtilities.saveCounters(fileSystem, HadoopUtilities.buildCounterFileName(this, conf), job);
            else
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
        ToolRunner.run(new SameClustererMerger(), args);
    }
}