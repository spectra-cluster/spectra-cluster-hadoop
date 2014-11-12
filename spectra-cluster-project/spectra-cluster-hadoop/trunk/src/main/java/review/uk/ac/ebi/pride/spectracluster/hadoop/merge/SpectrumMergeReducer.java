package review.uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class SpectrumMergeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
