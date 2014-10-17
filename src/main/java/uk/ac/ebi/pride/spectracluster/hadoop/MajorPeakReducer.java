package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.*;
import org.apache.hadoop.io.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.engine.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import javax.annotation.*;
import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.MajorPeakReducer
 * Form clusters from peaks
 */
public class MajorPeakReducer extends AbstractClusteringEngineReducer {



    private double majorPeakWindowSize = HadoopDefaults.getMajorPeakMZWindowSize();


    public double getMajorPeakWindowSize() {
        return majorPeakWindowSize;
    }



    @Override
    public void reduceNormal(Text key, Iterable<Text> values,
                             Context context) throws IOException, InterruptedException {

        String keyStr = key.toString();
        PeakMZKey mzKey = new PeakMZKey(keyStr);

        // if we are in a different bin - different charge or peak
        if (mzKey.getPeakMZ() != getMajorPeak()) {
            updateEngine(context, mzKey);
        }

        IIncrementalClusteringEngine engine = getEngine();
        if (engine == null)
            return; // very occasionally  we get null - not sure why

        int numberClusters = 0;
        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();


            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            final ISpectrum match = ParserUtilities.readMGFScan(rdr);
            if (match == null)
                continue; // not sure why this happens but nothing seems like the thing to do
            final ICluster cluster = ClusterUtilities.asCluster(match);

            if(numberClusters++ > 4)
                valStr = val.toString(); // break here

            final Collection<ICluster> removedClusters = engine.addClusterIncremental(cluster);

            writeClusters(context, removedClusters);


        }
    }

    /**
     * this version of writeCluster does all the real work
     * @param context
     * @param cluster
     * @throws IOException
     * @throws InterruptedException
     */
    protected void writeOneVettedCluster(@Nonnull final Context context,@Nonnull  final ICluster cluster) throws IOException, InterruptedException {
        MZKey key = new MZKey(cluster.getPrecursorMz());

        StringBuilder sb = new StringBuilder();
        final CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        String string = sb.toString();

        // make sure this has a few lines of text
        if (string.length() > SpectraHadoopUtilities.MIMIMUM_CLUSTER_LENGTH) {
            writeKeyValue(key.toString(), string, context);
            incrementBinCounters(key, context); // how big are the bins - used in next job
        }
    }

    protected void incrementBinCounters(MZKey mzKey, Context context) {
         IWideBinner binner = HadoopDefaults.DEFAULT_WIDE_MZ_BINNER;
         int[] bins = binner.asBins(mzKey.getPrecursorMZ());
         //noinspection ForLoopReplaceableByForEach
         for (int i = 0; i < bins.length; i++) {
             int bin = bins[i];
             SpectraHadoopUtilities.incrementPartitionCounter(context, "Bin", bin);

         }

     }



    protected <T> boolean updateEngine(final Context context, final T key) throws IOException, InterruptedException
      {
          PeakMZKey pMzKey = (PeakMZKey)key;

          if (getEngine() != null) {
              final Collection<ICluster> clusters = getEngine().getClusters();
              writeClusters(context, clusters);
              setEngine(null);
          }
          // if not at end make a new engine
          if (pMzKey != null) {
              setEngine(getFactory().getIncrementalClusteringEngine( (float)getMajorPeakWindowSize()));
              setMajorPeak(pMzKey.getPeakMZ());
          }
         return true;
     }



}
