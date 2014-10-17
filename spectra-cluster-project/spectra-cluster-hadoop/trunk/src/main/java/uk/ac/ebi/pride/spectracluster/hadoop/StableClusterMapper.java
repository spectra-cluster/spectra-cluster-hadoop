package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.systemsbiology.hadoop.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.StableClusterMapper
 * Handle stable semistable and unstable clusters differently
 *
 * User: Steve
 * Date: 8/14/13
 */
public class StableClusterMapper extends AbstractParameterizedMapper<Text> {


    public static final Random RND = new Random();
    public static final int WIDE_BIN_WIDTH = 16;
    public static final int WIDE_BIN_OVERLAP = 1;
    public static final int MAX_SPECTRA_PER_GROUP = 1000;


    public static IWideBinner BINNER = new SizedWideBinner(
            MZIntensityUtilities.HIGHEST_USABLE_MZ,
            WIDE_BIN_WIDTH,
            MZIntensityUtilities.LOWEST_USABLE_MZ,
            WIDE_BIN_OVERLAP);

    private Map<Integer, String[]> binToAllKeys = new HashMap<Integer, String[]>();
    private int numberMapCalls;
    private IClusterStabilityAssessor clusterStabilityAssessor = new CountBasedClusterStabilityAssessor();


    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        ISetableParameterHolder application = getApplication();
        ConfigurableProperties.setStableClusterSizeFromProperties(application);
        AbstractBinnedAPrioriPartitioner.setBinner(BINNER);
      }


    /**
     * return all posssible keys for a bin - this may be subfragmanets
     *
     * @param bin
     * @param context
     * @return
     */
    public String[] getBinKeys(int bin, int charge, final double mz, Context context) {
        // look up
        String[] ret = binToAllKeys.get(bin);
        if (ret != null)
            return ret;
        ret = buildBinKeys(bin, charge, mz, context);
        binToAllKeys.put(bin, ret);
        return ret;
    }

    private String[] buildBinKeys(int bin, int charge, final double mz, Context context) {
        if(numberMapCalls++ % 1000 == 0)
            System.gc(); // garbage collect to try to cure memory issues

        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        List<String> holder = new ArrayList<String>();
        //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
        int numberSpectraWithMZ = DaltonBinSize.getNumberSpectraWithMZ((int) mz, context.getConfiguration());
        int ng = getNumberGroups(numberSpectraWithMZ);
        for (int i = 0; i < ng; i++) {
            StableBinMZKey key = new StableBinMZKey(bin, i, mz);
            holder.add(key.toString());

        }

        String[] ret = new String[holder.size()];
        holder.toArray(ret);
        return ret;
    }


    protected int getNumberGroups(int totalSpectra) {

        return 1 + (totalSpectra / MAX_SPECTRA_PER_GROUP);
    }

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

        switch (clusters.length) {
            case 0:
                return;
            case 1:
                handleCluster(clusters[0], text, context);
                return;
            default:
                throw new IllegalStateException("We got " + clusters.length +
                        " clusters - expected only 1"); //
        }
    }

    protected void handleCluster(ICluster cluster, String value, Context context) {
        IWideBinner binner = BINNER;

        throw new UnsupportedOperationException("Fix This"); // ToDo
//        //noinspection ForLoopReplaceableByForEach
//        int precursorCharge = cluster.getPrecursorCharge();
//        double precursorMZ = cluster.getPrecursorMz();
//        boolean stable = clusterStabilityAssessor.isStable(cluster);
//        int numberSpectra = cluster.getClusteredSpectraCount();
//
//        int[] bins = binner.asBins(precursorMZ);
//        //noinspection ForLoopReplaceableByForEach
//        for (int j = 0; j < bins.length; j++) {
//            int bin = bins[j];
//            String[] keys = getBinKeys(bin, precursorCharge, precursorMZ, context);
//            if (stable) {
//                //noinspection ForLoopReplaceableByForEach
//                for (int k = 0; k < keys.length; k++) {
//                    String akey = keys[k];
//                    writeKeyValue(akey, value, context);
//
//                }
//
//            } else {
//                // choose a random key for unstable clusters
//                String akey;
//                if (keys.length == 1)
//                    akey = keys[0];
//                else
//                    akey = keys[RND.nextInt(keys.length)];
//
//                UnStableBinMZKey uKey = new UnStableBinMZKey(akey);
//                akey = uKey.toString();
//
//                writeKeyValue(akey, value, context);
//            }
//
//
//          }
    }


    public static final int NUMBER_REDUCERS = 300;

    // for debugging add a partitioning counter
    @SuppressWarnings("UnusedDeclaration")
    public void countHashValues(BinMZKey mzKey, Context context) {
        //       incrementPartitionCounters(mzKey, context);    //the reducer handle
        //      incrementDaltonCounters((int)mzKey.getPrecursorMZ(),context);
    }

    public void incrementDaltonCounters(int precursorMZ, Context context) {
        Counter counter = context.getCounter("Binning", MZIntensityUtilities.describeDaltons(precursorMZ));
        counter.increment(1);
    }

    @SuppressWarnings("UnusedDeclaration")
    public void incrementPartitionCounters(BinMZKey mzKey, Context context) {
        int partition = mzKey.getPartitionHash() % NUMBER_REDUCERS;

        Counter counter = context.getCounter("Partitioning", "Partition" + String.format("%03d", partition));
        counter.increment(1);
    }


}
