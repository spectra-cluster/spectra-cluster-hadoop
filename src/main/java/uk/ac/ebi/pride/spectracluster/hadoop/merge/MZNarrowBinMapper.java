package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;

/**
 * Mapper using narrow bins
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZNarrowBinMapper extends Mapper<Text, Text, Text, Text> {

    private IWideBinner binner = ClusterHadoopDefaults.getBinner();

    /**
     * Output object reuse
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        boolean offsetBins = context.getConfiguration().getBoolean("pride.cluster.offset.bins", false);
        if (offsetBins) {
            IWideBinner offSetHalf = (IWideBinner) getBinner().offSetHalf();
            setBinner(offSetHalf);
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        IWideBinner binner = getBinner();

        // parse cluster
        ICluster[] clusters = IOUtilities.parseClustersFromCGFString(text);

        for (ICluster cluster : clusters) {
            float precursorMz = cluster.getPrecursorMz();
            int[] bins = binner.asBins(precursorMz);

            for (int bin : bins) {
                BinMZKey binMZKey = new BinMZKey(bin, precursorMz);

                // increment partition counter
//                CounterUtilities.incrementPartitionCounter(context, binMZKey);

                keyOutputText.set(binMZKey.toString());
                valueOutputText.set(value.toString());
                context.write(keyOutputText, valueOutputText);
            }
        }
    }

    public IWideBinner getBinner() {
        return binner;
    }

    public void setBinner(IWideBinner binner) {
        this.binner = binner;
    }
}
