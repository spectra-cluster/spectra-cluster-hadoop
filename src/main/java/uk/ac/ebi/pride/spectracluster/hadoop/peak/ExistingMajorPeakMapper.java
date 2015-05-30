package uk.ac.ebi.pride.spectracluster.hadoop.peak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.PeakMZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.CounterUtilities;

import java.io.IOException;

/**
 * Mapper that gets the previous written highest peak and its cluster pass them to the reducer
 *
 * @author Rui Wang
 * @version $Id$
 */
@Deprecated // not adapted yet
public class ExistingMajorPeakMapper extends Mapper<Writable, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        // check the validity of the input
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        PeakMZKey peakMZKey = new PeakMZKey(key.toString());

        // increment dalton bin counter
        CounterUtilities.incrementMajorPeakCounters(context, peakMZKey.getPeakMZ());

        // write out cluster with its major peak from the input
        keyOutputText.set(peakMZKey.toString());
        valueOutputText.set(value.toString());
        context.write(keyOutputText, valueOutputText);
    }
}
