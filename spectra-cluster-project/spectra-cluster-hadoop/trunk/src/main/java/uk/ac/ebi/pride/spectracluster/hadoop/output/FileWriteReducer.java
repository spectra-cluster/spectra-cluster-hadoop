package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;

import java.io.IOException;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class FileWriteReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        MZKey mzKey = new MZKey(key.toString());


    }
}
