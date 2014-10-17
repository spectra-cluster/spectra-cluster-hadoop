package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.hadoop.*;
import com.lordjoe.hadoopsimulator.*;
import com.lordjoe.utilities.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.DuplicateReductionSimulator
 * User: Steve
 * Date: 4/13/2014
 */
public class DuplicateReductionSimulator {

    /**
     * implementation to return the key and value as a TextKeyValue array
     */
    public static ITextMapper IDENTITY_MAPPER = new ITextMapper() {
        @Override
        public TextKeyValue[] map(final String key, final String value, final Properties config) {
            TextKeyValue[] ret = new TextKeyValue[1];
            ret[0] = new TextKeyValue(key, value);
            return ret;
        }
    };

    public static int numberMappedBySpectrum;
    public static int numberReducedBySpectrum;
    public static int numberMappedByCluster;
    public static int numberReducedByCluster;


    /**
     * send each spectrum with its own id and all the clusters it is a member of
     */
    public static class SpectrumInClusterBySpectrumMapper implements ITextMapper {
        @SuppressWarnings("ConstantConditions")
        @Override
        public TextKeyValue[] map(String key, String value, Properties config) {
            String label = key.toString();
            String text = value.toString();
            if (label == null || text == null)
                return TextKeyValue.EMPTY_ARRAY;
            if (label.length() == 0 || text.length() == 0)
                return TextKeyValue.EMPTY_ARRAY;


            LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
            ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);

            switch (clusters.length) {
                case 0:
                    return TextKeyValue.EMPTY_ARRAY;
                case 1:
                    return handleCluster(clusters[0]);
                default:
                    throw new IllegalStateException("We got " + clusters.length +
                            " clusters - expected only 1"); //
            }
        }

        /**
         * write each spectrum as a SpectrumToCluster with the spectrum id as the key
         * This guarantees that all clusters containing a spectrum go to one place
         *
         * @param cluster
         */
        protected TextKeyValue[] handleCluster(ICluster cluster) {
            List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
            List<TextKeyValue> holder = new ArrayList<TextKeyValue>();

            for (ISpectrum sc : clusteredSpectra) {
                SpectrumInCluster spectrumInCluster = new SpectrumInCluster(sc, cluster);
                String id = sc.getId();
                StringBuilder sb = new StringBuilder();
                spectrumInCluster.append(sb);
                //           writeKeyValue(id, sb.toString(), context);
                holder.add(new TextKeyValue(id, sb.toString()));
                numberMappedBySpectrum++;

            }
            TextKeyValue[] ret = new TextKeyValue[holder.size()];
            holder.toArray(ret);
            return ret;

        }

    }

    /**
     * standard WordCount Reducer for out jobs
     */
    public static class SpectrumInClusterBySpectrumReducer implements ITextReducer {

        @Override
        public TextKeyValue[] reduce(String key, List<String> values, Properties config) {
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            String spectrumId = key.toString();
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            int numberProcessed = 0;

            // grab all spectraInClusters - these are guaranteed to be small
            List<SpectrumInCluster> passedClusters = getValuesToSpectrumInClusterList(values);
            int oldSize = passedClusters.size();
            if (oldSize == 0)
                return TextKeyValue.EMPTY_ARRAY;

            // drop smaller clusters contained in larger
            // Note - everywhere they are seem these will be dropped
            List<SpectrumInCluster> handler = SpectrumInCluster.dropContainedClusters(passedClusters);

            // Keep clusters but mark when the spectrum should be removed
            SpectrumInCluster.handleClusters(handler);
            int newSize = handler.size();
            if (newSize != oldSize)
                newSize = handler.size(); // brak here

            List<TextKeyValue> holder = new ArrayList<TextKeyValue>();

            for (SpectrumInCluster inCluster : handler) {
                String id = inCluster.getCluster().getSpectralId();
                StringBuilder sb = new StringBuilder();
                inCluster.append(sb);
                holder.add(new TextKeyValue(id, sb.toString()));
                numberMappedBySpectrum++;
                //    writeSpectrumInCluster(inCluster, context);
            }
            TextKeyValue[] ret = new TextKeyValue[holder.size()];
            holder.toArray(ret);
            return ret;
        }

        public static List<SpectrumInCluster> getValuesToSpectrumInClusterList(final List<String> values) {
            List<SpectrumInCluster> handler = new ArrayList<SpectrumInCluster>();


            //noinspection LoopStatementThatDoesntLoop
            for (String val : values) {
                String valStr = val.toString();
                LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
                SpectrumInCluster sci2 = SpectrumInClusterUtilities.readSpectrumInCluster(rdr);
                handler.add(sci2);
            }
            return handler;
        }
    }


    /**
     * standard WordCount Reducer for out jobs
     */
    public static class SpectrumInClusterByClusterReducer implements ITextReducer {

        @Override
        public TextKeyValue[] reduce(String key, List<String> values, Properties config) {
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment

            ICluster sc = new SpectralCluster((String)null, Defaults.getDefaultConsensusSpectrumBuilder());
            List<TextKeyValue> holder = new ArrayList<TextKeyValue>();
            Set<String> processedSpectrunIds = new HashSet<String>();

            String word = key;
            int count = 0;
            for (String value : values) {
                numberReducedByCluster++;
                LineNumberReader rdr = new LineNumberReader((new StringReader(value)));
                SpectrumInCluster sci2 = SpectrumInClusterUtilities.readSpectrumInCluster(rdr);
                ISpectrum spectrum = sci2.getSpectrum();
                String id = spectrum.getId();
                if (!sci2.isRemoveFromCluster()) {
                    sc.addSpectra(spectrum);
                } else {
                    // handle spectra kicked out
                    if (!processedSpectrunIds.contains(id)) {
                        ICluster cluster = ClusterUtilities.asCluster(spectrum);
                        StringBuffer sb = new StringBuffer();
                        cluster = ClusterUtilities.asCluster(spectrum);
                        TextKeyValue sumCount = new TextKeyValue(id, sb.toString());
                        holder.add(sumCount); // send as a singleton
                    } else {
                        System.out.println("duplicate id " + id);
                    }

                }
                processedSpectrunIds.add(id);
            }
            if (sc.getClusteredSpectraCount() == 0)
                return TextKeyValue.EMPTY_ARRAY;
            String id = sc.getSpectralId();
            StringBuffer sb = new StringBuffer();
            final CGFClusterAppender cgfClusterAppender = CGFClusterAppender.INSTANCE;
            cgfClusterAppender.appendCluster(sb, sc);
            TextKeyValue sumCount = new TextKeyValue(id, sb.toString());
            holder.add(sumCount);
            TextKeyValue[] ret = new TextKeyValue[holder.size()];
            holder.toArray(ret);
            return ret;
        }
    }


//    private static IClusterSet rebuildClusters(final List<TextKeyValue> pStep2) {
//        IClusterSet ret = new SimpleClusterSet();
//        for (TextKeyValue textKeyValue : pStep2) {
//            ICluster cluster = buildClusterFromKeyValue(textKeyValue);
//            if (cluster != null)
//                ret.addCluster(cluster);
//        }
//        return ret;
//    }

    private static ICluster buildClusterFromKeyValue(final TextKeyValue kv) {
        String text = kv.getValue();
        // System.out.println(text);
        LineNumberReader rdr = new LineNumberReader(new StringReader(text));
        ICluster cls = ParserUtilities.readSpectralCluster(rdr, null);
        return cls;
    }

    private static List<TextKeyValue> runClusterMapReduce(final List<TextKeyValue> pInput) {
        Properties unusedConfig = new Properties();
        HadoopSimulatorJob hj = new HadoopSimulatorJob(IDENTITY_MAPPER, new SpectrumInClusterByClusterReducer());
        List<TextKeyValue> ret = hj.runJob(pInput, unusedConfig);
        return ret;
    }

    private static List<TextKeyValue> runSpectrumMapReduce(final List<TextKeyValue> pInput) {
        Properties unusedConfig = new Properties();
        HadoopSimulatorJob hj = new HadoopSimulatorJob(new SpectrumInClusterBySpectrumMapper(), new SpectrumInClusterBySpectrumReducer());
        List<TextKeyValue> ret = hj.runJob(pInput, unusedConfig);
        return ret;
    }
//
//
//    public static IClusterSet simulateDuplicateReduction(IClusterSet in) {
//        ElapsedTimer timer = new ElapsedTimer();
//        List<TextKeyValue> input = clusterToTextValues(in);
//        timer.showElapsed("clusterToTextValues");
//        timer.reset();
//
//        List<TextKeyValue> step1 = runSpectrumMapReduce(input);
//        timer.showElapsed("runSpectrumMapReduce");
//        timer.reset();
//        input.clear(); // we cannot afford the memory
//
//        List<TextKeyValue> step2 = runClusterMapReduce(step1);
//        step1.clear(); // we cannot afford the memory
//        timer.showElapsed("runClusterMapReduce");
//        timer.reset();
//
//        IClusterSet ret = rebuildClusters(step2);
//        step2.clear(); // we cannot afford the memory
//        timer.showElapsed("rebuildClusters");
//        timer.reset();
//
//        return ret;
//    }
//
//    public static List<TextKeyValue> clusterToTextValues(final IClusterSet in) {
//        List<TextKeyValue> input = new ArrayList<TextKeyValue>();
//        List<ICluster> clusters = in.getClusters();
//        int index = 0;
//        final CGFClusterAppender clusterAppender = new CGFClusterAppender(new MGFSpectrumAppender());
//        for (ICluster cluster : clusters) {
//            StringBuilder sb = new StringBuilder();
//            clusterAppender.appendCluster(sb, cluster);
//            input.add(new TextKeyValue(Integer.toString(index++), sb.toString()));
//        }
//        return input;
//    }

    private static void usage() {
        System.out.println("<cgffile or cgf directory>   ...");
    }


    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        }
        ElapsedTimer timer = new ElapsedTimer();
        for (int i = 0; i < args.length; i++) {
//            String arg = args[i];
//            File originalFile = new File(arg);
//            if (!originalFile.exists())
//                throw new IllegalStateException("nonexistant cluster file " + arg);
//            timer.reset();
//            IClusterSet cs = ClusterSimilarityUtilities.createClusteringSetFromCGF(originalFile);
//
//            CountedMap<String> countedMap = ClusterSimilarityUtilities.getCountedMap(cs);
//            System.out.println(countedMap);
//            System.out.println();  // terminate dots display
//            timer.showElapsed("read " + arg);
//            timer.reset();
//            if (cs.getClusterCount() == 0)
//                throw new IllegalStateException("bad cluster file " + arg);
//            IClusterSet cs2 = DuplicateReductionSimulator.simulateDuplicateReduction(cs);
//            timer.showElapsed("processed " + arg);
//            timer.reset();
//            CountedMap<String> countedMap2 = ClusterSimilarityUtilities.getCountedMap(cs2);
//            System.out.println(countedMap2);
//            CountedMap<String> countedMap3 = countedMap.getDifferences(countedMap2);
//            System.out.println(countedMap3);
//
//            try {
//                PrintWriter out = new PrintWriter(new FileWriter(arg + ".clustering"));
//                final DotClusterClusterAppender clusterAppender = new DotClusterClusterAppender();
//                for (ICluster sc : cs.getClusters()) {
//                    clusterAppender.appendCluster(out, sc);
//                }
//                out.close();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            timer.showElapsed("written " + arg);
        }
    }


}
