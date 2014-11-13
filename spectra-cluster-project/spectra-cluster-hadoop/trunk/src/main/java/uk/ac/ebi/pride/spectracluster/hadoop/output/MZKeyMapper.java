package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

/**
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZKeyMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String label = key.toString();
        String text = value.toString();

        if (label.length() == 0 || text.length() == 0)
            return;

        LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);

        for (ICluster cluster : clusters) {
            MZKey mzkey = new MZKey(cluster.getPrecursorMz());
            String keyStr = mzkey.toString();
            context.write(new Text(keyStr), new Text(text));
        }
    }
}
