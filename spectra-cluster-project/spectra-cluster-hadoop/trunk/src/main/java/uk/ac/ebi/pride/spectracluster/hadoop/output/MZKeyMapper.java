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
 * Mapper that simply read clusters and pass them on using their precursor m/z as the key
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZKeyMapper extends Mapper<Text, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        LineNumberReader rdr = new LineNumberReader((new StringReader(value.toString())));
        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);

        for (ICluster cluster : clusters) {
            MZKey mzkey = new MZKey(cluster.getPrecursorMz());

            keyOutputText.set(mzkey.toString());
            valueOutputText.set(value.toString());
            context.write(keyOutputText, valueOutputText);
        }
    }
}
