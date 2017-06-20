package uk.ac.ebi.pride.spectracluster.hadoop.clustering;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.BinMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.ConfigurableProperties;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.util.MZIntensityUtilities;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;
import uk.ac.ebi.pride.spectracluster.util.binner.SizedWideBinner;

import java.io.IOException;

/**
 * This Mapper maps spectra / clusters based on their precursor m/z
 * value into bins based on the set bin size.
 */
public class PrecursorBinMapper extends Mapper<Text, Text, Text, Text> {

    private IWideBinner binner;
    public final float DEFAULT_BIN_WIDTH = 4.0F;

    /**
     * Output object reuse
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());

        float binWidth = context.getConfiguration().getFloat(MajorPeakJob.CURRENT_BINNER_WINDOW_SIZE, DEFAULT_BIN_WIDTH);

        setBinner(new SizedWideBinner(
                MZIntensityUtilities.HIGHEST_USABLE_MZ,
                binWidth,
                0,
                0,
                true));

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

            // must only be in one bin
            if (bins.length > 1) {
                throw new InterruptedException("Multiple bins found for " + String.valueOf(precursorMz));
            }
            else if (bins.length == 0) {
                continue;
            }

            BinMZKey binMZKey = new BinMZKey(bins[0], precursorMz);
            keyOutputText.set(binMZKey.toString());
            valueOutputText.set(value.toString());
            context.write(keyOutputText, valueOutputText);
        }
    }

    public IWideBinner getBinner() {
        return binner;
    }

    public void setBinner(IWideBinner binner) {
        this.binner = binner;
    }
}
