package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.io.*;
import org.systemsbiology.hadoop.*;

import java.io.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.BinCountCombiner
 * similar to word count but combines for different bins
 *
 * @author Steve Lewis
 * @date 30/10/13
 */
@SuppressWarnings("UnusedDeclaration")
public class BinCountCombiner extends AbstractParameterizedReducer {

    /**
     * null op for ordinary keys only special keys are combined
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduceNormal(Text key, Iterable<Text> values,
                                Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }

    }

      protected void reduceSpecial(Text key, Iterable<Text> values,
                                 Context context) throws IOException, InterruptedException {
       // Key is bin number with # prepended
        long sum = 0;
        for (Text val : values) {
            sum += Long.parseLong(val.toString());
        }
      //  Text onlyValue = getOnlyValue();
      //  onlyValue.set(Long.toString(sum));
      //  context.write(key, onlyValue);
          writeKeyValue(key.toString(),Long.toString(sum),context);
    }



}
