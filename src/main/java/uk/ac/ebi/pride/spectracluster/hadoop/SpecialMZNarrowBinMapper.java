package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.IWideBinner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.systemsbiology.hadoop.AbstractParameterizedMapper;
import org.systemsbiology.hadoop.HadoopUtilities;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.keys.MZKey;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.TextIdentityMapper
 * Not sure whay I cannot find in 0.2 but uses the key and value unchanged
 * User: Steve
 * Date: 8/14/13
 */
@SuppressWarnings("UnusedDeclaration")
public class SpecialMZNarrowBinMapper extends AbstractParameterizedMapper<Text> {

    private IWideBinner mapBinner;
     @Override
     protected void setup(final Context context) throws IOException, InterruptedException {
         super.setup(context);
         setMapBinner(HadoopDefaults.DEFAULT_WIDE_MZ_BINNER);
     }

     public IWideBinner getMapBinner() {
         return mapBinner;
     }

     public void setMapBinner(final IWideBinner pMapBinner) {
         mapBinner = pMapBinner;
         AbstractBinnedAPrioriPartitioner.setBinner(pMapBinner);
      }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String label = key.toString();
        String text = value.toString();
        if (label == null || text == null)
            return;
        if (label.length() == 0 || text.length() == 0)
            return;

        IWideBinner binner = getMapBinner();


        LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < clusters.length; i++) {
            ICluster cluster = clusters[i];
              double precursorMZ = cluster.getPrecursorMz();
            int[] bins = binner.asBins(precursorMZ);
            //noinspection ForLoopReplaceableByForEach
            for (int j = 0; j < bins.length; j++) {
                int bin = bins[j];
                BinMZKey mzKey = new BinMZKey(bin, precursorMZ);


                MZKey mzkey = new MZKey(cluster.getPrecursorMz());


                SpectraHadoopUtilities.incrementPartitionCounter(context, mzKey);   // debug to make sure partitioning is balanced

                //  if(bin != 149986)
                //         continue; // todo remove this is to debug one case

                // check partitioning
                countHashValues(mzKey, context);

                final String keyStr = mzkey.toString();
                writeKeyValue(keyStr, text, context);
            }
        }

    }


    // for debugging add a partitioning counter
    @SuppressWarnings("UnusedDeclaration")
    public void countHashValues(BinMZKey mzKey, Context context) {
        //      incrementPartitionCounters(mzKey, context);    //the reducer handle
    }

    @SuppressWarnings("UnusedDeclaration")
    public void incrementPartitionCounters(BinMZKey mzKey, Context context) {
        //noinspection ConstantIfStatement
        if (true)
            return;
        int partition = mzKey.getPartitionHash() % HadoopUtilities.DEFAULT_TEST_NUMBER_REDUCERS;

        Counter counter = context.getCounter("Partitioning", "Partition" + String.format("%03d", partition));
        counter.increment(1);
    }


}
