package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.io.*;
import org.systemsbiology.hadoop.*;


import java.io.*;

/**
* uk.ac.ebi.pride.spectracluster.hadoop.TextIdentityMapper
 * Not sure whay I cannot find in 0.2 but uses the key and value unchanged
* User: Steve
* Date: 8/14/13
*/
public class ChargeMZRegionMapper extends AbstractParameterizedMapper<Text> {


    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        context.write(key, value);
    }


}
