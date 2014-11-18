package uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ClusterHadoopDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

/**
 * Mapper using narrow bins
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZNarrowBinMapper extends Mapper<Text, Text, Text, Text> {

    private IWideBinner binner = ClusterHadoopDefaults.DEFAULT_WIDE_MZ_BINNER;

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
        LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);

        for (ICluster cluster : clusters) {
            float precursorMz = cluster.getPrecursorMz();
            int[] bins = binner.asBins(precursorMz);

            for (int bin : bins) {
                BinMZKey binMZKey = new BinMZKey(bin, precursorMz);

                // increment partition counter
                CounterUtilities.incrementPartitionCounter(context, binMZKey);

                String binMZKeyString = binMZKey.toString();
                context.write(new Text(binMZKeyString), value);
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
